#!/usr/bin/env node

import { KafkaProducer, KafkaProducerConfig } from '../src/producer'
import * as protobuf from 'protobufjs'
import * as path from 'path'

interface Args {
  brokers: string[]
  topic: string
  type: 'user-event' | 'product-update' | 'log-entry'
  clientId?: string
  continuous?: boolean
  interval?: number
  [key: string]: any
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
      default:
        // Store additional arguments for message properties
        if (flag.startsWith('--')) {
          parsed[flag.substring(2)] = value
        }
        break
    }
  }

  // Check for environment variables as fallback
  parsed.brokers = parsed.brokers || (process.env.KAFKA_BROKERS?.split(','))
  parsed.topic = parsed.topic || process.env.KAFKA_TOPIC
  parsed.type = parsed.type || (process.env.KAFKA_MESSAGE_TYPE as Args['type'])
  parsed.clientId = parsed.clientId || process.env.KAFKA_CLIENT_ID
  parsed.interval = parsed.interval || parseInt(process.env.KAFKA_INTERVAL || '1000')

  if (!parsed.brokers || !parsed.topic || !parsed.type) {
    console.error('Usage: protobuf-producer --brokers localhost:9092 --topic my-topic --type user-event [--client-id my-client] [--continuous] [--interval 1000] [message-specific-args]')
    console.error('')
    console.error('Options:')
    console.error('  --continuous: Send messages continuously until stopped (Ctrl+C)')
    console.error('  --interval: Milliseconds between messages in continuous mode (default: 1000)')
    console.error('')
    console.error('Message types:')
    console.error('  user-event: --user-id <id> --event-type <type> [--property-key value]')
    console.error('  product-update: --product-id <id> --name <name> --price <price> --inventory <count> --category <cat>')
    console.error('  log-entry: --level <level> --message <msg> --service <service> [--metadata-key value]')
    console.error('')
    console.error('Or set environment variables: KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_MESSAGE_TYPE, KAFKA_CLIENT_ID, KAFKA_INTERVAL')
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

function createMessage(args: Args, types: any, messageCount = 0) {
  const timestamp = Date.now()

  switch (args.type) {
    case 'user-event': {
      const properties: { [key: string]: string } = {}

      // Extract property-* arguments
      Object.keys(args).forEach(key => {
        if (key.startsWith('property-')) {
          properties[key.substring(9)] = args[key]
        }
      })

      // Add dynamic properties for continuous mode
      if (args.continuous) {
        properties.messageCount = messageCount.toString()
        properties.sessionId = `session_${Math.random().toString(36).substr(2, 9)}`
      }

      const payload = {
        userId: args['user-id'] || `user${messageCount % 100}`,
        eventType: args['event-type'] || ['login', 'logout', 'purchase', 'view'][messageCount % 4],
        timestamp,
        properties
      }

      const message = types.UserEvent.create(payload)
      return types.UserEvent.encode(message).finish()
    }

    case 'product-update': {
      const basePrice = parseFloat(args.price) || 29.99
      const baseInventory = parseInt(args.inventory) || 100

      const payload = {
        productId: args['product-id'] || `product_${messageCount % 50}`,
        name: args.name || `Product ${messageCount % 50}`,
        price: args.continuous ? Math.round((basePrice + (Math.random() * 10 - 5)) * 100) / 100 : basePrice,
        inventory: args.continuous ? Math.max(0, baseInventory + Math.floor(Math.random() * 20 - 10)) : baseInventory,
        category: args.category || ['electronics', 'clothing', 'books', 'home'][messageCount % 4],
        updatedAt: timestamp
      }

      const message = types.ProductUpdate.create(payload)
      return types.ProductUpdate.encode(message).finish()
    }

    case 'log-entry': {
      const metadata: { [key: string]: string } = {}

      // Extract metadata-* arguments
      Object.keys(args).forEach(key => {
        if (key.startsWith('metadata-')) {
          metadata[key.substring(9)] = args[key]
        }
      })

      // Add dynamic metadata for continuous mode
      if (args.continuous) {
        metadata.messageCount = messageCount.toString()
        metadata.requestId = `req_${Math.random().toString(36).substr(2, 9)}`
      }

      const levels = ['DEBUG', 'INFO', 'WARN', 'ERROR']
      const services = ['api-server', 'auth-service', 'payment-service', 'notification-service']
      const messages = ['Processing request', 'Operation completed', 'Cache miss', 'Database query', 'User authenticated', 'Request validated']

      const payload = {
        level: args.level || (args.continuous ? levels[messageCount % levels.length] : 'INFO'),
        message: args.message || (args.continuous ? `${messages[messageCount % messages.length]} #${messageCount}` : 'Sample log message'),
        service: args.service || (args.continuous ? services[messageCount % services.length] : 'example-service'),
        timestamp,
        metadata
      }

      const message = types.LogEntry.create(payload)
      return types.LogEntry.encode(message).finish()
    }

    default:
      throw new Error(`Unknown message type: ${args.type}`)
  }
}

async function main() {
  const args = parseArgs()

  const config: KafkaProducerConfig = {
    bootstrapServers: args.brokers,
    clientId: args.clientId,
  }

  const producer = new KafkaProducer(config)
  let messageCount = 0
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
      console.log(`Starting continuous ${args.type} sending to topic "${args.topic}" every ${args.interval}ms. Press Ctrl+C to stop.`)

      while (!isShuttingDown) {
        try {
          const messageBuffer = createMessage(args, types, ++messageCount)

          await producer.send(args.topic, messageBuffer)
          console.log(`[${messageCount}] Sent ${args.type} message (${messageBuffer.length} bytes)`)

          await new Promise(resolve => setTimeout(resolve, args.interval))
        } catch (error) {
          console.error('Error sending message:', error)
          await new Promise(resolve => setTimeout(resolve, args.interval))
        }
      }

      console.log(`\nSent ${messageCount} ${args.type} messages total`)
    } else {
      console.log('Creating protobuf message...')
      const messageBuffer = createMessage(args, types)
      console.log(`Created ${args.type} message (${messageBuffer.length} bytes)`)

      console.log(`Sending protobuf message to topic "${args.topic}"...`)
      await producer.send(args.topic, messageBuffer)
      console.log('Message sent successfully')
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