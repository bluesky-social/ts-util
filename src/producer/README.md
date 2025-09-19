# Kafka Producer

## Usage Example

```ts
const producer = new KafkaProducer({
  brokers: ['localhost:9092'],
  clientId: 'client-id',
})

await producer.connect()

const encoded = EventProtobuf.encode({
  val: 'value',
}).finish()

await producer.send('events', encoded, 'some-key')

await producer.disconnect()
```
