#!/usr/bin/env node

import { KafkaProducer, KafkaProducerConfig } from '../src/producer'
import * as protobuf from 'protobufjs'
import * as path from 'path'

interface Args {
  brokers: string[]
  topic: string
  count: number
  type: 'user-event' | 'product-update' | 'log-entry'
  clientId?: string
  continuous?: boolean
  interval?: number
}

function parseArgs(): Args {
  const args = process.argv.slice(2)
  const parsed: Partial<Args> = {}

  for (let i = 0; i < args.length; i += 2) {
    const flag = args[i]
    const value = args[i + 1]

    switch (flag) {
      case '--brokers':
        parsed.brokers = value.split(',')
        break
      case '--topic':
        parsed.topic = value
        break
      case '--count':
        parsed.count = parseInt(value)
        break
      case '--type':
        parsed.type = value as Args['type']
        break
      case '--client-id':
        parsed.clientId = value
        break
      case '--continuous':
        parsed.continuous = true
        i-- // No value needed for this flag
        break
      case '--interval':
        parsed.interval = parseInt(value)
        break
    }
  }

  // Check for environment variables as fallback
  parsed.brokers = parsed.brokers || (process.env.KAFKA_BROKERS?.split(','))
  parsed.topic = parsed.topic || process.env.KAFKA_TOPIC
  parsed.count = parsed.count || parseInt(process.env.KAFKA_MESSAGE_COUNT || '10')
  parsed.type = parsed.type || (process.env.KAFKA_MESSAGE_TYPE as Args['type']) || 'user-event'
  parsed.clientId = parsed.clientId || process.env.KAFKA_CLIENT_ID
  parsed.interval = parsed.interval || parseInt(process.env.KAFKA_INTERVAL || '5000')

  if (!parsed.brokers || !parsed.topic) {
    console.error('Usage: batch-producer --brokers localhost:9092 --topic my-topic [--count 100] [--type user-event] [--client-id my-client] [--continuous] [--interval 5000]')
    console.error('  --continuous: Send batches continuously until stopped (Ctrl+C)')
    console.error('  --interval: Milliseconds between batches in continuous mode (default: 5000)')
    console.error('Or set environment variables: KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_MESSAGE_COUNT, KAFKA_MESSAGE_TYPE, KAFKA_CLIENT_ID, KAFKA_INTERVAL')
    process.exit(1)
  }

  return parsed as Args
}

async function loadProtoSchema() {
  const protoPath = path.join(__dirname, 'schemas', 'message.proto')
  const root = await protobuf.load(protoPath)

  const UserEvent = root.lookupType('examples.UserEvent')
  const ProductUpdate = root.lookupType('examples.ProductUpdate')
  const LogEntry = root.lookupType('examples.LogEntry')

  return { UserEvent, ProductUpdate, LogEntry }
}

function createBatchMessages(count: number, type: Args['type'], types: any) {
  const messages: Array<{ value: Buffer; key?: string }> = []

  for (let i = 0; i < count; i++) {
    const timestamp = Date.now() + i
    let messageBuffer: Buffer
    let key: string | undefined

    switch (type) {
      case 'user-event': {
        const payload = {
          userId: `user${i % 100}`, // Cycle through 100 different users
          eventType: ['login', 'logout', 'purchase', 'view'][i % 4],
          timestamp,
          properties: {
            sessionId: `session_${Math.random().toString(36).substr(2, 9)}`,
            userAgent: 'batch-producer/1.0'
          }
        }

        const message = types.UserEvent.create(payload)
        messageBuffer = Buffer.from(types.UserEvent.encode(message).finish())
        key = payload.userId
        break
      }

      case 'product-update': {
        const payload = {
          productId: `product_${i % 50}`, // Cycle through 50 different products
          name: `Product ${i % 50}`,
          price: Math.round((Math.random() * 100 + 10) * 100) / 100, // $10-$110
          inventory: Math.floor(Math.random() * 1000),
          category: ['electronics', 'clothing', 'books', 'home'][i % 4],
          updatedAt: timestamp
        }

        const message = types.ProductUpdate.create(payload)
        messageBuffer = Buffer.from(types.ProductUpdate.encode(message).finish())
        key = payload.productId
        break
      }

      case 'log-entry': {
        const levels = ['DEBUG', 'INFO', 'WARN', 'ERROR']
        const services = ['api-server', 'auth-service', 'payment-service', 'notification-service']

        const payload = {
          level: levels[i % levels.length],
          message: `Batch message ${i + 1} - ${['Processing request', 'Operation completed', 'Cache miss', 'Database query'][i % 4]}`,
          service: services[i % services.length],
          timestamp,
          metadata: {
            requestId: `req_${Math.random().toString(36).substr(2, 9)}`,
            batchId: 'batch_001'
          }
        }

        const message = types.LogEntry.create(payload)
        messageBuffer = Buffer.from(types.LogEntry.encode(message).finish())
        key = payload.service
        break
      }

      default:
        throw new Error(`Unknown message type: ${type}`)
    }

    messages.push({ value: messageBuffer, key })
  }

  return messages
}

async function main() {
  const args = parseArgs()

  const config: KafkaProducerConfig = {
    bootstrapServers: args.brokers,
    clientId: args.clientId,
  }

  const producer = new KafkaProducer(config)
  let batchCount = 0
  let totalMessageCount = 0
  let isShuttingDown = false

  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT, shutting down gracefully...')
    isShuttingDown = true
  })

  try {
    console.log('Loading protobuf schema...')
    const types = await loadProtoSchema()
    console.log('Schema loaded successfully')

    console.log('Connecting to Kafka...')
    await producer.connect()
    console.log('Connected successfully')

    if (args.continuous) {
      console.log(`Starting continuous batch sending to topic "${args.topic}" every ${args.interval}ms.`)
      console.log(`Each batch contains ${args.count} ${args.type} messages. Press Ctrl+C to stop.`)

      while (!isShuttingDown) {
        try {
          const startTime = Date.now()

          console.log(`\n[Batch ${++batchCount}] Creating ${args.count} protobuf messages...`)
          const messages = createBatchMessages(args.count, args.type, types)
          const totalSize = messages.reduce((sum, msg) => sum + msg.value.length, 0)

          console.log(`[Batch ${batchCount}] Sending ${messages.length} messages (${totalSize} bytes)...`)
          await producer.sendBatch(args.topic, messages)

          const duration = Date.now() - startTime
          totalMessageCount += messages.length

          console.log(`[Batch ${batchCount}] Sent successfully in ${duration}ms (${Math.round(messages.length / (duration / 1000))} msg/sec)`)

          await new Promise(resolve => setTimeout(resolve, args.interval))
        } catch (error) {
          console.error(`[Batch ${batchCount}] Error:`, error)
          await new Promise(resolve => setTimeout(resolve, args.interval))
        }
      }

      console.log(`\nSent ${batchCount} batches with ${totalMessageCount} total messages`)
    } else {
      console.log(`Creating ${args.count} protobuf messages of type ${args.type}...`)
      const messages = createBatchMessages(args.count, args.type, types)
      const totalSize = messages.reduce((sum, msg) => sum + msg.value.length, 0)
      console.log(`Created ${messages.length} messages (${totalSize} total bytes)`)

      console.log(`Sending batch to topic "${args.topic}"...`)
      const startTime = Date.now()
      await producer.sendBatch(args.topic, messages)
      const duration = Date.now() - startTime

      console.log(`Batch sent successfully in ${duration}ms`)
      console.log(`Throughput: ${Math.round(messages.length / (duration / 1000))} messages/sec`)
    }

  } catch (error) {
    console.error('Error:', error)
    process.exit(1)
  } finally {
    await producer.disconnect()
    console.log('Disconnected')
  }
}

main().catch(console.error)