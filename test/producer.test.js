'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { KafkaProducer } = require('../lib/producer')

function deferred() {
  let resolve
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

function createProducer(options) {
  const producer = Object.create(KafkaProducer.prototype)
  producer.topic = 'events'
  producer.maxMessageBytes = 1_000_000
  producer.producer = options.producer
  producer.batcher = options.batching
    ? new (require('../lib/producer').KafkaBatcher)(producer, options.batching)
    : undefined
  return producer
}

test('KafkaProducer batches enqueued records and drains on disconnect', async () => {
  const firstSend = deferred()
  const calls = []
  let sendCount = 0
  let disconnectCount = 0
  const producer = createProducer({
    batching: { maxBatchRecords: 2, maxBatchWaitMs: 1_000 },
    producer: {
      send: ({ messages }) => {
        calls.push(messages)
        sendCount += 1
        return sendCount === 1 ? firstSend.promise : Promise.resolve()
      },
      disconnect: async () => {
        disconnectCount += 1
      },
    },
  })

  producer.enqueue(Buffer.from('one'), 'first')
  producer.enqueue(Buffer.from('two'), 'second')
  producer.enqueue(Buffer.from('three'), 'third')

  assert.deepEqual(calls[0], [
    { value: Buffer.from('one'), key: 'first' },
    { value: Buffer.from('two'), key: 'second' },
  ])

  firstSend.resolve()
  await producer.disconnect()

  assert.deepEqual(calls[1], [
    { value: Buffer.from('three'), key: 'third' },
  ])
  assert.equal(disconnectCount, 1)
})

test('KafkaProducer flushes batches at the byte threshold', async () => {
  const sent = deferred()
  const calls = []
  const producer = createProducer({
    batching: {
      maxBatchRecords: 100,
      maxBatchBytes: 6,
      maxBatchWaitMs: 1_000,
    },
    producer: {
      send: async ({ messages }) => {
        calls.push(messages)
        sent.resolve()
      },
      disconnect: async () => {},
    },
  })

  producer.enqueue(Buffer.from('one'), 'first')
  producer.enqueue(Buffer.from('two'), 'second')
  await sent.promise

  assert.deepEqual(calls, [
    [
      { value: Buffer.from('one'), key: 'first' },
      { value: Buffer.from('two'), key: 'second' },
    ],
  ])
  await producer.disconnect()
})

test('KafkaProducer flushes partial batches after the wait time', async () => {
  const sent = deferred()
  const calls = []
  const producer = createProducer({
    batching: { maxBatchRecords: 100, maxBatchWaitMs: 10 },
    producer: {
      send: async ({ messages }) => {
        calls.push(messages)
        sent.resolve()
      },
      disconnect: async () => {},
    },
  })

  producer.enqueue(Buffer.from('one'), 'first')
  await sent.promise

  assert.deepEqual(calls, [
    [{ value: Buffer.from('one'), key: 'first' }],
  ])
  await producer.disconnect()
})

test('KafkaProducer requires batching for enqueue', () => {
  const producer = createProducer({
    producer: { disconnect: async () => {} },
  })

  assert.throws(
    () => producer.enqueue(Buffer.from('one')),
    /batching is not configured/
  )
})
