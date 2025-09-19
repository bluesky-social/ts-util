# Kafka Producer Examples

This directory contains examples demonstrating how to use the Kafka producer with different message formats, including Protocol Buffers serialization.

## Setup

### Quick Start with Docker

1. **Start Kafka cluster:**
   ```bash
   cd examples
   docker-compose up -d
   ```

   This will start:
   - Zookeeper on port 2181
   - Kafka broker on port 9092
   - Kafka UI on port 8080 (http://localhost:8080)

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Run examples:**
   ```bash
   # Basic text message
   npx ts-node basic-producer.ts --brokers localhost:9092 --topic test --message "Hello Docker!"

   # Protobuf message
   npx ts-node protobuf-producer.ts --brokers localhost:9092 --topic events --type user-event --user-id user123 --event-type login

   # Batch messages
   npx ts-node batch-producer.ts --brokers localhost:9092 --topic batch --count 100 --type log-entry
   ```

4. **View messages in Kafka UI:**
   Open http://localhost:8080 in your browser to see topics and messages.

5. **Stop Kafka cluster:**
   ```bash
   docker-compose down
   ```

### SASL Authentication Setup

For testing with SASL Plain authentication:

1. **Start SASL-enabled Kafka cluster:**
   ```bash
   cd examples
   docker-compose -f docker-compose.sasl.yml up -d
   ```

   This starts Kafka with SASL Plain authentication on port 9093.

2. **Test with SASL authentication:**
   ```bash
   # Using command line arguments
   npx ts-node basic-producer.ts \
     --brokers localhost:9093 \
     --topic sasl-test \
     --message "Hello SASL!" \
     --sasl-username testuser \
     --sasl-password testpass

   # Using environment variables
   export KAFKA_BROKERS="localhost:9093"
   export KAFKA_SASL_USERNAME="testuser"
   export KAFKA_SASL_PASSWORD="testpass"
   npx ts-node basic-producer.ts --topic sasl-test --message "Hello SASL!"
   ```

3. **Available test users:**
   - `admin` / `admin-secret` (admin user)
   - `testuser` / `testpass` (test user)
   - `producer` / `producer-pass` (producer user)

4. **Stop SASL Kafka cluster:**
   ```bash
   docker-compose -f docker-compose.sasl.yml down
   ```

### Manual Kafka Setup

If you prefer to run Kafka manually or use an existing cluster:

1. Install dependencies:
   ```bash
   cd examples
   npm install
   ```

2. Make sure you have a Kafka cluster running. For local development, you can use:
   ```bash
   docker run -p 9092:9092 apache/kafka:2.8.0
   ```

## Examples

### 1. Basic Producer (`basic-producer.ts`)

Sends simple text messages to Kafka.

**Usage:**
```bash
npx ts-node basic-producer.ts --brokers localhost:9092 --topic test-topic --message "Hello World"
```

**Options:**
- `--brokers`: Comma-separated list of Kafka brokers
- `--topic`: Kafka topic name
- `--message`: Message content
- `--key`: Optional message key
- `--client-id`: Optional Kafka client ID
- `--continuous`: Send messages continuously until stopped (Ctrl+C)
- `--interval`: Milliseconds between messages in continuous mode (default: 1000)
- `--ssl`: Enable SSL/TLS
- `--sasl-username`: SASL Plain username
- `--sasl-password`: SASL Plain password

**Environment variables:**
- `KAFKA_BROKERS`: Kafka brokers
- `KAFKA_TOPIC`: Topic name
- `KAFKA_MESSAGE`: Message content
- `KAFKA_KEY`: Message key
- `KAFKA_CLIENT_ID`: Client ID
- `KAFKA_INTERVAL`: Message interval for continuous mode
- `KAFKA_SSL`: Enable SSL (set to 'true')
- `KAFKA_SASL_USERNAME`: SASL username
- `KAFKA_SASL_PASSWORD`: SASL password

### 2. Protocol Buffers Producer (`protobuf-producer.ts`)

Sends structured messages serialized with Protocol Buffers.

**Usage:**

**User Event:**
```bash
npx ts-node protobuf-producer.ts \
  --brokers localhost:9092 \
  --topic user-events \
  --type user-event \
  --user-id user123 \
  --event-type login \
  --property-sessionId sess_abc123
```

**Product Update:**
```bash
npx ts-node protobuf-producer.ts \
  --brokers localhost:9092 \
  --topic product-updates \
  --type product-update \
  --product-id prod456 \
  --name "Gaming Mouse" \
  --price 49.99 \
  --inventory 150 \
  --category electronics
```

**Log Entry:**
```bash
npx ts-node protobuf-producer.ts \
  --brokers localhost:9092 \
  --topic logs \
  --type log-entry \
  --level INFO \
  --message "User authentication successful" \
  --service auth-service \
  --metadata-requestId req_xyz789
```

### 3. Batch Producer (`batch-producer.ts`)

Sends multiple messages in a single batch for better performance.

**Usage:**
```bash
npx ts-node batch-producer.ts \
  --brokers localhost:9092 \
  --topic test-batch \
  --count 1000 \
  --type user-event
```

**Options:**
- `--count`: Number of messages to generate and send (default: 10)
- `--type`: Message type (`user-event`, `product-update`, or `log-entry`)

## Message Schemas

The protobuf schema is defined in `schemas/message.proto` and includes three message types:

### UserEvent
- `user_id`: String identifier for the user
- `event_type`: Type of event (login, logout, purchase, etc.)
- `timestamp`: Unix timestamp
- `properties`: Key-value map for additional event properties

### ProductUpdate
- `product_id`: String identifier for the product
- `name`: Product name
- `price`: Product price
- `inventory`: Available inventory count
- `category`: Product category
- `updated_at`: Unix timestamp

### LogEntry
- `level`: Log level (DEBUG, INFO, WARN, ERROR)
- `message`: Log message
- `service`: Name of the service generating the log
- `timestamp`: Unix timestamp
- `metadata`: Key-value map for additional log metadata

## Configuration

All examples support configuration via command-line arguments or environment variables:

**Environment Variables:**
- `KAFKA_BROKERS`: Comma-separated broker list
- `KAFKA_TOPIC`: Default topic name
- `KAFKA_CLIENT_ID`: Client identifier
- `KAFKA_MESSAGE_TYPE`: Message type for protobuf examples
- `KAFKA_MESSAGE_COUNT`: Number of messages for batch example

**Example with environment variables:**
```bash
export KAFKA_BROKERS="broker1:9092,broker2:9092"
export KAFKA_TOPIC="my-topic"
export KAFKA_CLIENT_ID="my-producer"

npx ts-node basic-producer.ts --message "Hello from env config"
```

## Performance Tips

1. Use batch sending for better throughput when sending multiple messages
2. Configure appropriate message compression in the producer config
3. Use message keys for partitioning when order matters
4. Consider message size limits (default: 1MB)

## Troubleshooting

1. **Connection errors**: Verify Kafka brokers are running and accessible
2. **Topic not found**: Create topics manually or enable auto-creation
3. **Message too large**: Check `maxMessageBytes` configuration
4. **Protobuf errors**: Ensure schema files are accessible and valid