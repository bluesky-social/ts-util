import { Kafka, Producer, KafkaConfig, Admin } from 'kafkajs'
import os from 'node:os'

const DEFAULT_CLIENT_ID = 'kafka-producer'

export type SASLConfig = {
  mechanism: 'plain'
  username: string
  password: string
}

export type KafkaProducerConfig = {
  // Kafka client
  bootstrapServers: string[]
  clientId: string
  requestTimeout?: number
  ssl: boolean
  sasl?: SASLConfig

  // Producer
  topic: string
  idempotent?: boolean
  retries?: number
  maxInFlightRequests?: number
  maxRetryTime?: number
  maxMessageBytes?: number

  // Topic
  numPartitions?: number
  replicationFactor?: number
}

const DEFAULT_CONFIG: Partial<KafkaProducerConfig> = {
  requestTimeout: 30_000, // Default timeout in KafkaJS
  idempotent: true,
  retries: 3,
  maxInFlightRequests: 5, // Default in KafkaJS
  maxRetryTime: 15_000, // Lower than the KafkaJS default of 30s
  maxMessageBytes: 1_000_000, // 1MB
  numPartitions: 20,
  replicationFactor: 1,
}

export class KafkaProducer {
  private config: KafkaProducerConfig
  private producer: Producer
  private topic: string
  private maxMessageBytes: number
  private admin: Admin

  private constructor(config: KafkaProducerConfig) {
    this.config = config
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
      requestTimeout: config.requestTimeout,
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
      idempotent: config.idempotent,
      maxInFlightRequests: config.maxInFlightRequests,
      retry: {
        retries: config.retries,
        initialRetryTime: 300,
        maxRetryTime: config.maxRetryTime,
        multiplier: 2,
      },
    })

    this.admin = kafka.admin()
  }

  static async create(config: KafkaProducerConfig): Promise<KafkaProducer> {
    const fullConfig = {
      ...DEFAULT_CONFIG,
      ...config,
    }

    const producer = new KafkaProducer(fullConfig)
    await producer.connect()
    await producer.ensureTopicExists()
    return producer
  }

  async connect(): Promise<void> {
    await this.producer.connect()
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect()
  }

  private async ensureTopicExists(): Promise<void> {
    const topics = await this.admin.listTopics()

    if (!topics.includes(this.topic)) {
      await this.admin.createTopics({
        topics: [
          {
            topic: this.config.topic,
            numPartitions: this.config.numPartitions,
            replicationFactor: this.config.replicationFactor,
          },
        ],
      })
    }
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
