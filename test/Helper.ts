/* tslint:disable:no-shadowed-variable */
/* tslint:disable:no-console */
import * as assert from 'assert'
import * as mage from 'mage'
import {
  ICouchbaseUnlockOptions,
  Helper
} from '../src'

// Couchbase mock
import {
  N1qlQuery
} from 'couchbase'

const couchbase = require('couchbase').Mock
const cluster = new couchbase.Cluster()
const client = cluster.openBucket('hi')

client.query = function (...args: any[]) {
  const callback = args.pop().bind(null, null, [], {})
  process.nextTick(callback)
}

// The mock does not have the _name attribute used
// by the helper to figure out the bucket name
client._name = 'hi'

// We override the standard Cluster class; we need
// to do this so that instanceof calls will resolve properly
require('couchbase').Cluster = couchbase.Cluster

// Archivist mocks
mage.core.archivist.getTopicApi = function (topicName: string, _vaultName: string): mage.archivist.ITopicApi | null {
  if (topicName === 'doesNotExist') {
    return null
  }

  const index = ['id']

  if (topicName === 'hasNoCreateKey') {
    return {
      index
    }
  }

  return {
    index,
    createKey(topicName: string, index: mage.archivist.IArchivistIndex) {
      return `${topicName}/id:${index.id}`
    }
  }
}

mage.core.archivist.getPersistentVaults = function () {
  return {
    isNotCouchbase: {},
    couchVault: {
      cluster,
      client
    }
  }
}

function assertState(state: any) {
  assert(state)
  assert(state.cas)
}

describe('Helper', function () {
  let helper: Helper
  beforeEach(() => helper = new Helper('couchVault', {
    maxAttempts: 2,
    retryInterval: 10
  }))

  describe('getVault', function () {
    it('throws if the underlying vault is not defined', function () {
      const helper = new Helper('willNeverExist')

      try {
        helper.getVault()
      } catch (error) {
        assert.equal(error.message, 'Vault willNeverExist does not exist')
        return
      }

      throw new Error('Did not fail')
    })

    it('throws if the underlying vault is not a CouchbaseVault', function () {
      const helper = new Helper('isNotCouchbase')

      try {
        helper.getVault()
      } catch (error) {
        assert.equal(error.message, 'Vault isNotCouchbase does not appear to be a Couchbase Vault')
        return
      }

      throw new Error('Did not fail')
    })

    it('returns a vault instance', function () {
      const vault = helper.getVault()
      assert(vault.cluster instanceof couchbase.Cluster)
    })

    it('vault instance is a singleton', function () {
      assert.strictEqual((<any> helper)._vault, undefined)
      helper.getVault()
      assert.notStrictEqual((<any> helper)._vault, undefined)
      helper.getVault() // make sure we hit the cache at least once
    })
  })

  describe('getKeyPrefix', function () {
    it('returns a key prefix', function () {
      const keyPrefix = helper.getKeyPrefix()
      assert.equal(keyPrefix, '')
    })

    it('key prefix is a singleton', function () {
      assert.strictEqual((<any> helper)._keyPrefix, undefined)
      helper.getKeyPrefix()
      assert.notStrictEqual((<any> helper)._keyPrefix, undefined)
      helper.getKeyPrefix() // make sure we hit the cache at least once
    })
  })

  describe('getQueryPool', function () {
    it('pool instance is a singleton', function () {
      assert.equal((<any> helper)._vault, null)
      helper.getQueryPool()
      helper.getQueryPool() // called twice to make sure we get the cached instance
    })
  })

  describe('getBucketName', function () {
    it('returns the correct bucket name', function () {
      assert.equal(helper.getBucketName(), 'hi')
    })
  })

  describe('createKey', function () {
    it('throws if no topic APIs are found', function () {
      try {
        const ret = helper.createKey('doesNotExist', {})
        console.log(ret)
      } catch (error) {
        assert.equal(error.message, 'Failed to fetch topic API, or topic API is missing the createKey function!')

        return
      }

      throw new Error('did not fail')
    })

    it('throws if the topic API does not hold a createKey method', function () {
      try {
        const ret = helper.createKey('hasNoCreateKey', {})
        console.log(ret)
      } catch (error) {
        assert.equal(error.message, 'Failed to fetch topic API, or topic API is missing the createKey function!')

        return
      }

      throw new Error('did not fail')
    })

    it('returns a key generated from a given tipuc and string', function () {
      const id = '123'
      const key = helper.createKey('fakeTopic', { id })

      assert.equal(key, 'fakeTopic/id:123')
    })
  })

  describe('query', function () {
    const oldQuery = client.query
    afterEach(() => client.query = oldQuery)

    it('works', async function () {
      const ret = await helper.query('select 1')

      assert.deepEqual(ret, {
        rows: [],
        meta: {}
      })
    })

    it('Can set consistency', async function () {
      client.query = function (query: any, ...args: any[]) {
        assert.equal(query.options.scan_consistency, 'request_plus')

        const callback = args.pop().bind(null, null, [], {})
        process.nextTick(callback)
      }

      await helper.query('select 1', {
        consistency: N1qlQuery.Consistency.REQUEST_PLUS
      })
    })

    it('Can set adhoc', async function () {
      client.query = function (query: any, ...args: any[]) {
        assert.equal(query.isAdhoc, false)

        const callback = args.pop().bind(null, null, [], {})
        process.nextTick(callback)
      }

      await helper.query('select 1', {
        adhoc: false
      })
    })

    it('Can pass a state', async function () {
      client.query = function (query: any, ...args: any[]) {
        assert.equal(query.options.scan_consistency, 'at_plus')
        assert.equal(query.options.scan_vectors.cas, 123)

        const callback = args.pop().bind(null, null, [], {})
        process.nextTick(callback)
      }

      await helper.query('select 1', null, {
        cas: 123
      })
    })
  })

  describe('add', function () {
    it('works', async () => {
      const state = await helper.add('fakeTopic', { id: '1' }, 'val')
      assertState(state)
    })
  })

  describe('set', function () {
    it('works', async () => {
      const state = await helper.set('fakeTopic', { id: '1' }, 'val')
      assertState(state)
    })
  })

  describe('replace', function () {
    it('works', async () => {
      const state = await helper.replace('fakeTopic', { id: '1' }, 'val')
      assertState(state)
    })
  })

  describe('remove', function () {
    it('works', async () => {
      const state = await helper.remove('fakeTopic', { id: '1' })
      assertState(state)
    })
  })

  describe('incr', function () {
    it('works', async () => {
      await helper.set('fakeTopic', { id: '1' }, 1)
      const data = await helper.incr('fakeTopic', { id: '1' }, 1)

      assert.equal(data.value, 2)
    })
  })

  describe('get', function () {
    it('throws on failure (promiseRespond)', async function () {
      const index = { id: '1' }

      try {
        await helper.remove('fakeTopic', index)
      } catch (error) {
        // do nothing
      }

      try {
        await helper.get('fakeTopic', index)
      } catch (error) {
        return
      }

      throw new Error('did not fail')
    })

    it('works', async function () {
      const index = { id: '1' }
      const val = 'asdfasfasdfasdfas'

      await helper.set('fakeTopic', index, val)
      const data = await helper.get('fakeTopic', index)

      assert.equal(data.value, val)
    })
  })

  describe('lock', function () {
    const index = { id: '1' }

    let lock: ICouchbaseUnlockOptions
    afterEach(async () => {
      if (lock && lock.cas) {
        await helper.unlock('fakeTopic', index, lock)
      }
    })

    it('throws if we try to lock a non-exising key (retriablePromiseRespond)', async function () {
      try {
        await helper.remove('fakeTopic', index)
      } catch (error) {
        // do nothing
      }

      try {
        await helper.lock('fakeTopic', index)
      } catch (error) {
        return
      }

      throw new Error('Did not fail')
    })

    it('on lock failure, retries (retriablePromiseRespond)', async function () {
      await helper.set('fakeTopic', index, 'ok')
      lock = await helper.lock('fakeTopic', index)

      process.nextTick(async () => await helper.unlock('fakeTopic', index, lock))

      // This will block until we unlock;
      lock = await helper.lock('fakeTopic', index)
    })

    it('works', async function () {
      await helper.set('fakeTopic', index, 'ok')
      lock = await helper.lock('fakeTopic', index)
    })
  })

  describe('unlock', function () {
    it('works', async function () {
      const index = { id: '1' }

      await helper.set('fakeTopic', index, 'ok')
      const lock = await helper.lock('fakeTopic', index)

      await helper.unlock('fakeTopic', index, lock)
    })
  })

  describe('dropIndexes', function () {
    it('works', async () => await helper.dropIndexes())
  })

  describe('view', function () {
    it('works', async () => await helper.view('designDocument', 'viewName'))

    it('options requiring stringification are stringified', async () => {
      await helper.view('designDocument', 'viewName', {
        startkey: ['1']
      })
    })
  })
})
