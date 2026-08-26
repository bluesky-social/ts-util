'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { KafkaProducer, KafkaProducerClosedError } = require('../lib/producer')

function deferred() {
  let resolve
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

function createProducer(config, send, disconnect = async () => {}) {
  const { Kafka } = require('kafkajs')
  const original = Kafka.prototype.producer
  Kafka.prototype.producer = () => ({
    connect: async () => {},
    send,
    disconnect,
  })
  try {
    return KafkaProducer.create({
      bootstrapServers: ['localhost:9092'],
      topic: 'events',
      ...config,
    })
  } finally {
    Kafka.prototype.producer = original
  }
}

test('batches records and drains bounded batches on disconnect', async () => {
  const firstSend = deferred()
  const calls = []
  const producer = await createProducer(
    { batching: { maxBatchRecords: 2, maxBatchWaitMs: 1_000 } },
    ({ messages }) => {
      calls.push(messages)
      return calls.length === 1 ? firstSend.promise : Promise.resolve()
    }
  )

  for (const value of ['a', 'b', 'c', 'd', 'e']) {
    producer.enqueue(Buffer.from(value), value)
  }
  firstSend.resolve()
  await producer.disconnect()

  assert.ok(calls.every((messages) => messages.length <= 2))
  assert.deepEqual(
    calls.flat().map(({ key }) => key),
    ['a', 'b', 'c', 'd', 'e']
  )
})

test('flushes when the byte threshold is reached', async () => {
  const sent = deferred()
  const calls = []
  const producer = await createProducer(
    {
      batching: {
        maxBatchRecords: 100,
        maxBatchBytes: 8,
        maxBatchWaitMs: 1_000,
      },
    },
    async ({ messages }) => {
      calls.push(messages)
      sent.resolve()
    }
  )

  producer.enqueue(Buffer.from('one'), 'first')
  await sent.promise

  assert.deepEqual(calls[0], [
    { value: Buffer.from('one'), key: 'first' },
  ])
  await producer.disconnect()
})

test('rejects enqueue after disconnect starts', async () => {
  const send = deferred()
  const producer = await createProducer(
    { batching: { maxBatchRecords: 1 } },
    () => send.promise
  )

  producer.enqueue(Buffer.from('one'))
  const disconnect = producer.disconnect()
  assert.throws(
    () => producer.enqueue(Buffer.from('two')),
    KafkaProducerClosedError
  )
  send.resolve()
  await disconnect
})

test('continues after send and error handler failures', async () => {
  let calls = 0
  const producer = await createProducer(
    {
      batching: {
        maxBatchRecords: 1,
        onError: async () => {
          throw new Error('handler failed')
        },
      },
    },
    async () => {
      calls += 1
      if (calls === 1) throw new Error('send failed')
    }
  )

  producer.enqueue(Buffer.from('one'))
  await new Promise((resolve) => setImmediate(resolve))
  producer.enqueue(Buffer.from('two'))
  await producer.disconnect()

  assert.equal(calls, 2)
})
