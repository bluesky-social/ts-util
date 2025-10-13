import { Kafka, Producer, KafkaConfig } from 'kafkajs'
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
  idempotent?: boolean
  retries?: number
  maxInFlightRequests?: number
  maxRetryTime?: number
  maxMessageBytes?: number
  requestTimeout?: number
  ssl?: boolean
  sasl?: SASLConfig
}

export class KafkaProducer {
  private producer: Producer
  private topic: string
  private maxMessageBytes: number

  private constructor(config: KafkaProducerConfig) {
    this.topic = config.topic
    this.maxMessageBytes = config.maxMessageBytes || 1_000_000

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
      retry: {
        retries: config.retries ?? 5,
        initialRetryTime: 300,
        maxRetryTime: config.maxRetryTime ?? 30_000,
        multiplier: 2,
      },
    })
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
    await this.producer.disconnect()
  }

  async send(value: Buffer | Uint8Array, key?: string): Promise<void> {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value)

    if (buffer.length > this.maxMessageBytes) {
      throw new Error(
        `Message size ${buffer.length} bytes exceeds max ${this.maxMessageBytes} bytes`
      )
    }

    await this.producer.send({
      topic: this.topic,
      messages: [{ key, value: buffer }],
    })
  }
}
