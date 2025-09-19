#!/usr/bin/env node

import { KafkaProducer, KafkaProducerConfig } from '../src/producer'

interface Args {
  brokers: string[]
  topic: string
  message: string
  key?: string
  clientId?: string
  continuous?: boolean
  interval?: number
  ssl?: boolean
  saslUsername?: string
  saslPassword?: string
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
      case '--message':
        parsed.message = value
        break
      case '--key':
        parsed.key = value
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
      case '--ssl':
        parsed.ssl = true
        i-- // No value needed for this flag
        break
      case '--sasl-username':
        parsed.saslUsername = value
        break
      case '--sasl-password':
        parsed.saslPassword = value
        break
    }
  }

  // Check for environment variables as fallback
  parsed.brokers = parsed.brokers || (process.env.KAFKA_BROKERS?.split(','))
  parsed.topic = parsed.topic || process.env.KAFKA_TOPIC
  parsed.message = parsed.message || process.env.KAFKA_MESSAGE
  parsed.key = parsed.key || process.env.KAFKA_KEY
  parsed.clientId = parsed.clientId || process.env.KAFKA_CLIENT_ID
  parsed.interval = parsed.interval || parseInt(process.env.KAFKA_INTERVAL || '1000')
  parsed.ssl = parsed.ssl || (process.env.KAFKA_SSL === 'true')
  parsed.saslUsername = parsed.saslUsername || process.env.KAFKA_SASL_USERNAME
  parsed.saslPassword = parsed.saslPassword || process.env.KAFKA_SASL_PASSWORD

  if (!parsed.brokers || !parsed.topic || !parsed.message) {
    console.error('Usage: basic-producer --brokers localhost:9092 --topic my-topic --message "Hello World" [options]')
    console.error('')
    console.error('Options:')
    console.error('  --key <key>              Optional message key')
    console.error('  --client-id <id>         Kafka client ID')
    console.error('  --continuous             Send messages continuously until stopped (Ctrl+C)')
    console.error('  --interval <ms>          Milliseconds between messages in continuous mode (default: 1000)')
    console.error('  --ssl                    Enable SSL/TLS')
    console.error('  --sasl-username <user>   SASL Plain username')
    console.error('  --sasl-password <pass>   SASL Plain password')
    console.error('')
    console.error('Environment variables:')
    console.error('  KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_MESSAGE, KAFKA_KEY, KAFKA_CLIENT_ID,')
    console.error('  KAFKA_INTERVAL, KAFKA_SSL, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD')
    process.exit(1)
  }

  return parsed as Args
}

async function main() {
  const args = parseArgs()

  const config: KafkaProducerConfig = {
    bootstrapServers: args.brokers,
    clientId: args.clientId,
    ssl: args.ssl,
  }

  // Add SASL configuration if username/password provided
  if (args.saslUsername && args.saslPassword) {
    config.sasl = {
      mechanism: 'plain',
      username: args.saslUsername,
      password: args.saslPassword,
    }
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
    console.log('Connecting to Kafka...')
    await producer.connect()
    console.log('Connected successfully')

    if (args.continuous) {
      console.log(`Starting continuous sending to topic "${args.topic}" every ${args.interval}ms. Press Ctrl+C to stop.`)

      while (!isShuttingDown) {
        try {
          const timestamp = new Date().toISOString()
          const messageWithCount = `${args.message} [${++messageCount}] at ${timestamp}`

          await producer.send(args.topic, Buffer.from(messageWithCount), args.key)
          console.log(`[${messageCount}] Message sent: ${messageWithCount.substring(0, 50)}${messageWithCount.length > 50 ? '...' : ''}`)

          await new Promise(resolve => setTimeout(resolve, args.interval))
        } catch (error) {
          console.error('Error sending message:', error)
          await new Promise(resolve => setTimeout(resolve, args.interval))
        }
      }

      console.log(`\nSent ${messageCount} messages total`)
    } else {
      console.log(`Sending single message to topic "${args.topic}"...`)
      await producer.send(args.topic, Buffer.from(args.message), args.key)
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