import {
  Kafka,
  Producer,
  CompressionTypes,
  Message,
  KafkaJSConnectionError,
} from 'kafkajs'
import os from 'node:os'

const DEFAULT_CLIENT_ID = 'kafka-producer'

export type SASLConfig = {
  mechanism: 'plain'
  username: string
  password: string
}

export type KafkaProducerConfig = {
  bootstrapServers: string[]
  topic: string
  clientId?: string
  compression?: CompressionTypes
  idempotent?: boolean
  retries?: number
  maxMessageBytes?: number
  ssl?: boolean
  sasl?: SASLConfig
}

export class KafkaProducer {
  private producer: Producer
  private topic: string
  private connected = false
  private maxMessageBytes: number

  constructor(config: KafkaProducerConfig) {
    this.topic = config.topic
    this.maxMessageBytes = config.maxMessageBytes || 1_000_000

    if (!config.clientId) {
      const hostname = os.hostname()
      if (hostname) {
        config.clientId = `${hostname};host_override=${config.bootstrapServers[0]}`
      } else {
        config.clientId = `${DEFAULT_CLIENT_ID};host_override=${config.bootstrapServers[0]}`
      }
    }

    const { bootstrapServers, clientId, ssl, sasl } = config

    const kafkaConfig: any = {
      brokers: bootstrapServers,
      clientId,
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
      maxInFlightRequests: 5,
      retry: {
        retries: config.retries ?? 5,
        initialRetryTime: 300,
        maxRetryTime: 30_000,
        multiplier: 2,
      },
    })

    this.producer.on('producer.connect', () => {
      this.connected = true
    })

    this.producer.on('producer.disconnect', () => {
      this.connected = false
    })
  }

  async connect(): Promise<void> {
    if (!this.connected) {
      await this.producer.connect()
    }
  }

  async disconnect(): Promise<void> {
    if (this.connected) {
      await this.producer.disconnect()
      this.connected = false
    }
  }

  async send(value: Buffer | Uint8Array, key?: string): Promise<void> {
    await this.ensureConnected()

    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value)

    if (buffer.length > this.maxMessageBytes) {
      throw new Error(
        `Message size ${buffer.length} bytes exceeds max ${this.maxMessageBytes} bytes`
      )
    }

    try {
      await this.producer.send({
        topic: this.topic,
        messages: [{ key, value: buffer }],
      })
    } catch (error: any) {
      if (error instanceof KafkaJSConnectionError) {
        this.connected = false
        throw new Error(`Connection lost to Kafka broker: ${error.message}`)
      }
      throw error
    }
  }

  async sendBatch(
    messages: Array<{ value: Buffer | Uint8Array; key?: string }>
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('Not connected. Call connect() first.')
    }

    const kafkaMessages: Message[] = messages.map((msg, index) => {
      const buffer = Buffer.isBuffer(msg.value)
        ? msg.value
        : Buffer.from(msg.value)

      if (buffer.length > this.maxMessageBytes) {
        throw new Error(
          `Message ${index} size ${buffer.length} bytes exceeds max ${this.maxMessageBytes} bytes`
        )
      }

      return {
        key: msg.key,
        value: buffer,
      }
    })

    await this.producer.send({
      topic: this.topic,
      messages: kafkaMessages,
    })
  }

  async ensureConnected(maxRetries = 3) {
    for (let i = 0; i < maxRetries; i++) {
      if (this.connected) return
      try {
        await this.connect()
        return
      } catch (error) {
        if (i === maxRetries - 1) throw error
        await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)))
      }
    }
  }
}
