import { Kafka, Producer, CompressionTypes, Message } from 'kafkajs'
import os from 'node:os'

const DEFAULT_CLIENT_ID = 'kafka-producer'

export type SASLConfig = {
  mechanism: 'plain'
  username: string
  password: string
}

export type KafkaProducerConfig = {
  bootstrapServers: string[]
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
  private connected = false
  private maxMessageBytes: number

  constructor(config: KafkaProducerConfig) {
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
      retry: {
        retries: config.retries ?? 5,
      },
    })
  }

  async connect(): Promise<void> {
    if (!this.connected) {
      await this.producer.connect()
      this.connected = true
    }
  }

  async disconnect(): Promise<void> {
    if (this.connected) {
      await this.producer.disconnect()
      this.connected = false
    }
  }

  async send(
    topic: string,
    value: Buffer | Uint8Array,
    key?: string
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('Not connected. Call connect() first.')
    }

    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value)

    if (buffer.length > this.maxMessageBytes) {
      throw new Error(
        `Message size ${buffer.length} bytes exceeds max ${this.maxMessageBytes} bytes`
      )
    }

    await this.producer.send({
      topic,
      messages: [{ key, value: buffer }],
    })
  }

  async sendBatch(
    topic: string,
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
      topic,
      messages: kafkaMessages,
    })
  }
}
