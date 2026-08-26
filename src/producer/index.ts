import { CompressionTypes, Kafka, Producer, KafkaConfig } from 'kafkajs'
import os from 'node:os'

const DEFAULT_CLIENT_ID = 'kafka-producer'
const DEFAULT_MAX_BATCH_RECORDS = 100
const DEFAULT_MAX_BATCH_BYTES = 1_000_000
const DEFAULT_MAX_BATCH_WAIT_MS = 100

export type SASLConfig = {
  mechanism: 'plain'
  username: string
  password: string
}

export type KafkaProducerConfig = {
  bootstrapServers: string[]
  topic: string
  clientId?: string
  idempotent?: boolean
  retries?: number
  maxInFlightRequests?: number
  maxRetryTime?: number
  maxMessageBytes?: number
  requestTimeout?: number
  connectionTimeout?: number
  metadataMaxAge?: number
  compression?: CompressionTypes
  batching?: KafkaBatchingConfig
  ssl?: boolean
  sasl?: SASLConfig
}

type KafkaProducerMessage = {
  value: Buffer | Uint8Array
  key?: string
}

export type KafkaBatchingConfig = {
  // Maximum records per batch.
  maxBatchRecords?: number
  // Maximum bytes per batch.
  maxBatchBytes?: number
  // Maximum time to wait before sending a partial batch.
  maxBatchWaitMs?: number
  // Handles terminal background send failures. Failed batches are dropped.
  onError?: (error: unknown) => void | Promise<void>
}

export class KafkaProducerClosedError extends Error {
  constructor() {
    super('Kafka producer is closed')
    this.name = 'KafkaProducerClosedError'
  }
}

export class KafkaProducer {
  private producer: Producer
  private topic: string
  private maxMessageBytes: number
  private compression?: CompressionTypes
  private batcher?: KafkaBatcher

  private constructor(config: KafkaProducerConfig) {
    this.topic = config.topic
    this.maxMessageBytes = config.maxMessageBytes || 1_000_000
    this.compression = config.compression

    if (!config.clientId) {
      const hostname = os.hostname()
      const brokerHostname = config.bootstrapServers[0].split(':')[0]
      if (hostname) {
        config.clientId = `${hostname};host_override=${brokerHostname}`
      } else {
        config.clientId = `${DEFAULT_CLIENT_ID};host_override=${brokerHostname}`
      }
    }

    const { bootstrapServers, clientId, ssl, sasl } = config

    const kafkaConfig: KafkaConfig = {
      brokers: bootstrapServers,
      clientId,
      requestTimeout: config.requestTimeout ?? 30_000,
      connectionTimeout: config.connectionTimeout,
    }

    if (ssl) {
      kafkaConfig.ssl = true
    }

    if (sasl) {
      kafkaConfig.sasl = {
        mechanism: sasl.mechanism,
        username: sasl.username,
        password: sasl.password,
      }
    }

    const kafka = new Kafka(kafkaConfig)

    this.producer = kafka.producer({
      idempotent: config.idempotent ?? true,
      maxInFlightRequests: config.maxInFlightRequests ?? 5,
      metadataMaxAge: config.metadataMaxAge,
      retry: {
        retries: config.retries ?? 5,
        initialRetryTime: 300,
        maxRetryTime: config.maxRetryTime ?? 30_000,
        multiplier: 2,
      },
    })

    if (config.batching) {
      this.batcher = new KafkaBatcher(
        (messages) => this.sendMessages(messages),
        config.batching
      )
    }
  }

  static async create(config: KafkaProducerConfig): Promise<KafkaProducer> {
    const producer = new KafkaProducer(config)
    await producer.connect()
    return producer
  }

  async connect(): Promise<void> {
    await this.producer.connect()
  }

  async disconnect(): Promise<void> {
    await this.batcher?.drain()
    await this.producer.disconnect()
  }

  enqueue(value: Buffer | Uint8Array, key?: string): void {
    if (!this.batcher) {
      throw new Error('Kafka producer batching is not configured')
    }

    this.batcher.enqueue(this.prepareValue(value), key)
  }

  async send(value: Buffer | Uint8Array, key?: string): Promise<void> {
    await this.sendMessages([{ value, key }])
  }

  private async sendMessages(messages: KafkaProducerMessage[]): Promise<void> {
    const kafkaMessages = messages.map(({ value, key }) => ({
      key,
      value: this.prepareValue(value),
    }))

    await this.producer.send({
      topic: this.topic,
      messages: kafkaMessages,
      compression: this.compression,
    })
  }

  private prepareValue(value: Buffer | Uint8Array): Buffer {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value)
    if (buffer.length > this.maxMessageBytes) {
      throw new Error(
        `Message size ${buffer.length} bytes exceeds max ${this.maxMessageBytes} bytes`
      )
    }
    return buffer
  }
}

function requireInt(name: string, value: number, min: number): number {
  if (!Number.isInteger(value) || value < min) {
    throw new RangeError(`${name} must be an integer >= ${min}`)
  }
  return value
}

class KafkaBatcher {
  private readonly maxBatchRecords: number
  private readonly maxBatchBytes: number
  private readonly maxBatchWaitMs: number
  private readonly onError?: (error: unknown) => void
  private pending: KafkaProducerMessage[] = []
  private pendingBytes = 0
  private timer?: ReturnType<typeof setTimeout>
  private sendPromise?: Promise<void>
  private closed = false

  constructor(
    private readonly sendMessages: (
      messages: KafkaProducerMessage[]
    ) => Promise<void>,
    options: KafkaBatchingConfig = {}
  ) {
    this.maxBatchRecords = requireInt(
      'maxBatchRecords',
      options.maxBatchRecords ?? DEFAULT_MAX_BATCH_RECORDS,
      1
    )
    this.maxBatchBytes = requireInt(
      'maxBatchBytes',
      options.maxBatchBytes ?? DEFAULT_MAX_BATCH_BYTES,
      1
    )
    this.maxBatchWaitMs = requireInt(
      'maxBatchWaitMs',
      options.maxBatchWaitMs ?? DEFAULT_MAX_BATCH_WAIT_MS,
      0
    )
    this.onError = options.onError
  }

  enqueue(value: Buffer | Uint8Array, key?: string): void {
    // Reject late writes instead of silently losing them during shutdown.
    if (this.closed) throw new KafkaProducerClosedError()

    this.pending.push({ value, key })
    this.pendingBytes += this.messageBytes(value, key)
    if (
      this.pending.length >= this.maxBatchRecords ||
      this.pendingBytes >= this.maxBatchBytes
    ) {
      this.startFlush()
    } else if (!this.timer) {
      this.timer = setTimeout(() => this.startFlush(), this.maxBatchWaitMs)
    }
  }

  async drain(): Promise<void> {
    this.closed = true
    this.clearTimer()
    while (this.pending.length > 0 || this.sendPromise) {
      this.startFlush()
      await this.sendPromise
    }
  }

  private startFlush(): void {
    if (this.pending.length === 0 || this.sendPromise) return

    this.clearTimer()
    const messages = this.takeBatch()
    this.sendPromise = this.sendMessages(messages).catch((error) => {
      try {
        void Promise.resolve(this.onError?.(error)).catch(() => {})
      } catch {
        // Ignore callback failures.
      }
    })
    this.sendPromise.finally(() => {
      this.sendPromise = undefined
      if (this.pending.length > 0) this.startFlush()
    })
  }

  // Drain one batch bounded by the record and byte limits. Records that pile up
  // during an in-flight send stay queued for the next batch, so a burst can't
  // produce a single oversized request.
  private takeBatch(): KafkaProducerMessage[] {
    let count = 0
    let bytes = 0
    while (count < this.pending.length && count < this.maxBatchRecords) {
      const { value, key } = this.pending[count]
      const next = bytes + this.messageBytes(value, key)
      // Always take at least one so a record larger than maxBatchBytes can't stall the queue.
      if (count > 0 && next > this.maxBatchBytes) break
      bytes = next
      count++
    }
    this.pendingBytes -= bytes
    return this.pending.splice(0, count)
  }

  private messageBytes(value: Buffer | Uint8Array, key?: string): number {
    return value.byteLength + (key ? Buffer.byteLength(key) : 0)
  }

  private clearTimer(): void {
    if (!this.timer) return
    clearTimeout(this.timer)
    this.timer = undefined
  }
}
