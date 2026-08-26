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

export type KafkaProducerMessage = {
  value: Buffer | Uint8Array
  key?: string
}

export type KafkaBatchingConfig = {
  maxBatchRecords?: number
  maxBatchBytes?: number
  maxBatchWaitMs?: number
  onError?: (error: unknown) => void
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
      this.batcher = new KafkaBatcher(this, config.batching)
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
    await this.batcher?.flush()
    await this.producer.disconnect()
  }

  enqueue(value: Buffer | Uint8Array, key?: string): void {
    if (!this.batcher) {
      throw new Error('Kafka producer batching is not configured')
    }
    this.batcher.add(value, key)
  }

  async send(value: Buffer | Uint8Array, key?: string): Promise<void> {
    await this.sendBatch([{ value, key }])
  }

  async sendBatch(messages: KafkaProducerMessage[]): Promise<void> {
    const kafkaMessages = messages.map(({ value, key }) => {
      const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value)

      if (buffer.length > this.maxMessageBytes) {
        throw new Error(
          `Message size ${buffer.length} bytes exceeds max ${this.maxMessageBytes} bytes`
        )
      }

      return { key, value: buffer }
    })

    await this.producer.send({
      topic: this.topic,
      messages: kafkaMessages,
      compression: this.compression,
    })
  }
}

export class KafkaBatcher {
  private readonly maxBatchRecords: number
  private readonly maxBatchBytes: number
  private readonly maxBatchWaitMs: number
  private readonly onError?: (error: unknown) => void
  private pending: KafkaProducerMessage[] = []
  private pendingBytes = 0
  private timer?: ReturnType<typeof setTimeout>
  private flushPromise?: Promise<void>
  private closed = false

  constructor(
    private readonly producer: KafkaProducer,
    options: KafkaBatchingConfig = {}
  ) {
    this.maxBatchRecords = options.maxBatchRecords ?? DEFAULT_MAX_BATCH_RECORDS
    this.maxBatchBytes = options.maxBatchBytes ?? DEFAULT_MAX_BATCH_BYTES
    this.maxBatchWaitMs = options.maxBatchWaitMs ?? DEFAULT_MAX_BATCH_WAIT_MS
    this.onError = options.onError
  }

  add(value: Buffer | Uint8Array, key?: string): void {
    if (this.closed) return

    this.pending.push({ value, key })
    this.pendingBytes += value.byteLength
    if (
      this.pending.length >= this.maxBatchRecords ||
      this.pendingBytes >= this.maxBatchBytes
    ) {
      this.startFlush()
    } else if (!this.timer) {
      this.timer = setTimeout(() => this.startFlush(), this.maxBatchWaitMs)
    }
  }

  async flush(): Promise<void> {
    this.closed = true
    this.clearTimer()
    while (this.pending.length > 0 || this.flushPromise) {
      this.startFlush()
      await this.flushPromise
    }
  }

  private startFlush(): void {
    if (this.pending.length === 0 || this.flushPromise) return

    this.clearTimer()
    const messages = this.pending
    this.pending = []
    this.pendingBytes = 0
    this.flushPromise = this.producer.sendBatch(messages).catch((error) => {
      this.onError?.(error)
    })
    this.flushPromise.finally(() => {
      this.flushPromise = undefined
      if (this.pending.length > 0) this.startFlush()
    })
  }

  private clearTimer(): void {
    if (!this.timer) return
    clearTimeout(this.timer)
    this.timer = undefined
  }
}
