# Kafka Producer

## Usage Example

```ts
const producer = new KafkaProducer({
  brokers: ['localhost:9092'],
  clientId: 'client-id',
  topic: 'topic-name',
})

await producer.connect()

const evt = new EventProtobuf({})

await producer.send('events', evt.toBinary(), 'some-key')

await producer.disconnect()
```
