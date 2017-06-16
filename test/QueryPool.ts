/* tslint:disable:no-shadowed-variable */
import * as assert from 'assert'
import {
  DEFAULT_MAX_POOL_SIZE,
  DEFAULT_MAX_QUEUE_SIZE,
  DEFAULT_RETRY_INTERVAL,
  DEFAULT_RETRY_MAX_ATTEMPTS,
  QueryPool
} from '../src'

// Couchbase mock
const couchbase = require('couchbase').Mock
const cluster = new couchbase.Cluster()
const client = cluster.openBucket()

// query mock
client.query = function (...args: any[]) {
  const callback = args.pop().bind(null, null, [], {})
  process.nextTick(callback)
}

describe('QueryPool', function () {
  // MAGE sets up uncaughtException hooks, we need to remove them!
  process.removeAllListeners()

  describe('defaults', function () {
    const pool: QueryPool = new QueryPool(client)

    it('uses DEFAULT_MAX_QUEUE_SIZE', () => {
      assert.equal(pool.config.maxQueueSize, DEFAULT_MAX_QUEUE_SIZE)
    })

    it('uses DEFAULT_MAX_POOL_SIZE', () => {
      assert.equal(pool.config.maxPoolSize, DEFAULT_MAX_POOL_SIZE)
    })

    it('uses DEFAULT_RETRY_INTERVAL', () => {
      assert.equal(pool.config.retryInterval, DEFAULT_RETRY_INTERVAL)
    })

    it('uses DEFAULT_RETRY_MAX_ATTEMPTS', () => {
      assert.equal(pool.config.maxAttempts, DEFAULT_RETRY_MAX_ATTEMPTS)
    })
  })

  describe('options', function () {
    const maxPoolSize = 100
    const maxQueueSize = 200
    const maxAttempts = 123
    const retryInterval = 456

    const pool: QueryPool = new QueryPool(client, {
      maxPoolSize,
      maxQueueSize,
      maxAttempts,
      retryInterval
    })

    it('uses the configured max queue size', () => {
      assert.equal(pool.config.maxQueueSize, maxQueueSize)
    })

    it('uses the configured max pool size', () => {
      assert.equal(pool.config.maxPoolSize, maxPoolSize)
    })


    it('uses the retry interval value', () => {
      assert.equal(pool.config.retryInterval, retryInterval)
    })

    it('uses max retry value', () => {
      assert.equal(pool.config.maxAttempts, maxAttempts)
    })
  })

  describe('execute', function () {
    let pool: QueryPool

    beforeEach(() => pool = new QueryPool(client))

    it('throws if queue length reaches max pool size', async function () {
      const query = couchbase.N1qlQuery.fromString('SELECT 1')
      const queries = new Array(pool.config.maxQueueSize + 1)
        .fill(null)
        .map(async () => pool.execute(query))

      try {
        await Promise.all(queries)
      } catch (error) {
        assert.equal(error.message, 'QueryPool queue is full')
        return
      }

      throw new Error('Queue error was not triggered')
    })

    it('queries are retried', async function () {
      const rows = [1, 2, 3]
      const meta = true
      const client = cluster.openBucket()

      let tryCount = 0
      client.query = function (...args: any[]) {
        const callback = args.pop()
        callback(tryCount ? null : new Error('boom'), rows, meta)
        tryCount += 1
      }

      const pool = new QueryPool(client, {
        maxAttempts: 2,
        retryInterval: 1
      })

      const query = couchbase.N1qlQuery.fromString('SELECT 1')
      const res = await pool.execute(query)

      assert.equal(tryCount, 2)
      assert.equal(res.rows[0], 1)
      assert.equal(res.meta, true)
    })

    it('queries fail on max retry', async function () {
      const client = cluster.openBucket()

      let tryCount = 0
      client.query = function (...args: any[]) {
        const callback = args.pop()
        callback(new Error('boom'), null, null)
        tryCount += 1
      }

      const pool = new QueryPool(client, {
        maxAttempts: 2,
        retryInterval: 1
      })

      const query = couchbase.N1qlQuery.fromString('SELECT 1')

      try {
        await pool.execute(query)
      } catch (error) {
        assert.equal(error.message, 'boom')
        assert.equal(tryCount, 2)

        return
      }

      throw new Error('query retry did not fail')
    })
  })
})
