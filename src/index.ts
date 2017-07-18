import { queue, retry } from 'async'

import * as couchbase from 'couchbase'
import { Cluster, CouchbaseError, N1qlQuery, ViewQuery } from 'couchbase'

import * as mage from 'mage'
import { archivist } from 'mage'

export const DEFAULT_MAX_QUEUE_SIZE = 20
export const DEFAULT_MAX_POOL_SIZE = 20

export const DEFAULT_RETRY_INTERVAL = 1000
export const DEFAULT_RETRY_MAX_ATTEMPTS = 5

/**
 * Async sleep function
 *
 * @param {number} time
 * @returns
 */
async function sleep(time: number) {
  return new Promise((resolve) => setTimeout(resolve, time))
}

/**
 * Used by locking Helper methods, but could
 * also be used to make any Helper methods retryable
 *
 * @template T
 * @param {(data: any) => void} resolve
 * @param {() => Promise<T>} retry
 * @param {(error: any) => void} reject
 * @param {*} error
 * @param {*} data
 * @returns
 */
async function retriablePromiseRespond<T>(
  resolve: (data: any) => void,
  retry: () => Promise<T>,
  reject: (error: any) => void,
  error: any,
  data: any) {

  if (error) {
    // If error is not temporary, fail
    if ((<CouchbaseError> error).code !== 11) {
      return reject(error)
    }

    // Keep retrying when getting temporary failures
    try {
      data = await retry()
    } catch (error) {
      /* istanbul ignore next */
      return reject(error)
    }
  }

  // Resolve
  return resolve(data)
}

/**
 * Managed callbacks for promises
 *
 * @param {(data: any) => void} resolve
 * @param {(error: any) => void} reject
 * @param {*} error
 * @param {*} data
 * @returns
 */
function promiseRespond(resolve: (data: any) => void, reject: (error: any) => void, error: any, data: any) {
  if (error) {
    return reject(error)
  }

  resolve(data)
}

/**
 * Interface to define how the client can be internally accessed.
 *
 * @export
 * @interface ICouchbaseVault
 */
export interface ICouchbaseVault {
  client: couchbase.Bucket,
  cluster: couchbase.Cluster
}

/**
 * Value returned by Couchbase on `get`
 *
 * @export
 * @interface ICouchbaseValue
 */
export interface ICouchbaseValue {
  value: any,
  cas: any
}

/**
 * Option object to use on unlock
 *
 * @export
 * @interface ICouchbaseUnlockOptions
 */
export interface ICouchbaseUnlockOptions {
  cas: couchbase.Bucket.CAS
}

/**
 * Options for configuring the query pool
 * as well as the query behavior for retriable
 * queries (lock, unlock, etc)
 *
 * @export
 * @interface IQueryOptions
 */
export interface IQueryOptions {
  maxAttempts?: number,
  retryInterval?: number,
  maxPoolSize?: number
  maxQueueSize?: number
}

/**
 * Configuration the query pool
 * as well as the query behavior for retriable
 * queries (lock, unlock, etc)
 *
 * @export
 * @interface IQueryConfiguration
 */
export interface IQueryConfiguration {
  maxAttempts: number,
  retryInterval: number,
  maxPoolSize: number
  maxQueueSize: number
}

/**
 * Interface for the data being returned by our query methods
 *
 * @export
 * @interface IQueryResponse
 */
export interface IQueryResponse {
  rows: any[],
  meta: couchbase.Bucket.ViewQueryResponse.Meta
}

/**
 * QueryPool is used to locally rate-limit the number of concurrent queries
 * being executed at any given time.
 *
 * @export
 * @class QueryPool
 */
export class QueryPool {
  public config: IQueryConfiguration
  public client: couchbase.Bucket
  public queue: AsyncQueue<couchbase.N1qlQuery>

  /**
   * Creates an instance of QueryPool
   *
   * Upon creation, we create the pool queue `execute`
   * will be pushing tasks into.
   *
   * @param {couchbase.Bucket} client
   * @param {IQueryOptions} [options={}]
   *
   * @memberof QueryPool
   */
  constructor(client: couchbase.Bucket, options: IQueryOptions = {}) {
    this.config = {
      maxAttempts: options.maxAttempts || DEFAULT_RETRY_MAX_ATTEMPTS,
      retryInterval: options.retryInterval || DEFAULT_RETRY_INTERVAL,
      maxQueueSize: options.maxQueueSize || DEFAULT_MAX_QUEUE_SIZE,
      maxPoolSize: options.maxPoolSize || DEFAULT_MAX_POOL_SIZE
    }

    this.client = client
    this.queue = queue((query: couchbase.N1qlQuery, queueCallback: AsyncResultCallback<IQueryResponse, {} | Error>) => {
      retry({
        times: this.config.maxAttempts,
        interval: this.config.retryInterval
      }, (retryCallback) => {
        this.client.query(query, (error, rows, meta) => {
          if (error) {
            return retryCallback(error)
          }

          return retryCallback(undefined, { rows, meta })
        })
      }, queueCallback)
    }, this.config.maxPoolSize)
  }

  /**
   * Execute a query using the query pool
   *
   * @param {couchbase.N1qlQuery} query
   * @returns {Promise <IQueryResponse>}
   *
   * @memberof QueryPool
   */
  public async execute(query: couchbase.N1qlQuery): Promise <IQueryResponse> {
    const maxQueueSize = this.config.maxQueueSize

    if (this.queue.length() >= maxQueueSize) {
      throw new Error('QueryPool queue is full')
    }

    return new Promise<IQueryResponse>((resolve, reject) => {
      this.queue.push(query, (error: Error | null, response: IQueryResponse) => {
        promiseRespond(resolve, reject, error, response)
      })
    })
  }
}

/**
 * Helpers are used to help developers directly connect
 * to Couchbase; this is often required for accessing views
 * or running N1QL queries, but may also be needed in some cases
 * to run direct operations on buckets
 *
 * @export
 * @class Helper
 *
 */
export class Helper {
  public vaultName: string
  public config: IQueryConfiguration

  private _vault: ICouchbaseVault
  private _queryPool: QueryPool
  private _keyPrefix: string

  /**
   * Creates an instance of Helper
   *
   * @param {string} vaultName
   * @param {IQueryOptions} [options]
   *
   * @memberof Helper
   */
  constructor(vaultName: string, options: IQueryOptions = {}) {
    this.vaultName = vaultName
    this.config = {
      maxAttempts: options.maxAttempts || DEFAULT_RETRY_MAX_ATTEMPTS,
      retryInterval: options.retryInterval || DEFAULT_RETRY_INTERVAL,
      maxQueueSize: options.maxQueueSize || DEFAULT_MAX_QUEUE_SIZE,
      maxPoolSize: options.maxPoolSize || DEFAULT_MAX_POOL_SIZE
    }
  }

  /**
   * Retrieve the related vault instance
   *
   * @returns {couchbase.ICouchbaseVault}
   * @memberof Helper
   */
  public getVault() {
    if (!this._vault) {
      const persistentVaults = mage.core.archivist.getPersistentVaults()
      this._vault = persistentVaults[this.vaultName]

      if (!this._vault) {
        throw new Error('Vault ' + this.vaultName + ' does not exist')
      }

      if ((this._vault.cluster instanceof Cluster) === false) {
        throw new Error('Vault ' + this.vaultName + ' does not appear to be a Couchbase Vault')
      }
    }

    return this._vault
  }

  /**
   * Retrieve the key prefix to access data
   *
   * @returns {string}
   * @memberof Helper
   */
  public getKeyPrefix() {
    if (this._keyPrefix === undefined) {
      const vault = <any> this.getVault()
      this._keyPrefix = vault.keyPrefix || ''
    }

    return this._keyPrefix
  }

  /**
   * Retrieve the QueryPool instance for this helper instance
   *
   * @returns {QueryPool}
   * @memberof Helper
   */
  public getQueryPool() {
    if (!this._queryPool) {
      this._queryPool = new QueryPool(this.getVault().client, this.config)
    }

    return this._queryPool
  }

  /**
   * Retrieve the vault underlying bucket's name
   *
   * @returns {string} The name of the buckets
   * @memberof CouchbaseHelper
   */
  public getBucketName() {
    return (<any> this.getVault().client)._name
  }

  /**
   * Returns vault of given vaultName and key associated to given topic and index
   *
   * @param {String} vaultName The name of the vault
   * @param {String} topic The topic to access
   * @param {Object} index The index to fetch
   * @returns {{vault: Vault, key: String}} Object containing vault object and key string
   * @memberof CouchbaseHelper
   */
  public createKey(topic: string, index: archivist.IArchivistIndex) {
    // Get vault and topic api
    const topicApi = mage.core.archivist.getTopicApi(topic, this.vaultName)

    if (!topicApi || !topicApi.createKey) {
      throw new Error('Failed to fetch topic API, or topic API is missing the createKey function!')
    }

    return `${this.getKeyPrefix()}${topicApi.createKey(topic, index)}`
  }

  /**
   * Run a N1QL query on a given vault (bucket)
   *
   * @param {String} vaultName Name of the vault to query
   * @param {String} queryString N1QL query
   * @param {Function} cb Callback function
   * @returns {undefined} void
   * @memberof CouchbaseHelper
   */
  public async query(queryString: string) {
    const queryObj = N1qlQuery.fromString(queryString)

    return this.getQueryPool().execute(queryObj)
  }

  /**
   * Directly lock a topic value, by index
   *
   * @param {String} vaultName Vault name
   * @param {String} topic Topic in the vault
   * @param {Object} index Index to lock
   * @param {Object} options Locking options
   * @param {Function} cb Callback function
   * @returns {undefined} void
   * @memberof CouchbaseHelper
   */
  public async lock(topic: string, index: archivist.IArchivistIndex, options: couchbase.GetAndLockOptions = {}) {
    const self = this
    const key = this.createKey(topic, index)
    const {
      retryInterval
    } = this.config

    async function retry() {
      await sleep(retryInterval)
      return self.lock(topic, index, options)
    }

    return new Promise<ICouchbaseValue>((resolve, reject) => {
      this.getVault().client.getAndLock(key, options, async (error, data) => {
        await retriablePromiseRespond(resolve, retry, reject, error, data)
      })
    })
  }

  /**
   * Directly unlock a topic value, by index
   *
   * @param {String} topic Topic in the vault
   * @param {Object} index Index to lock
   * @param {Object} options Locking options
   * @param {Function} cb Callback function
   * @memberof CouchbaseHelper
   */
  public async unlock(topic: string, index: archivist.IArchivistIndex, options: ICouchbaseUnlockOptions) {
    const key = this.createKey(topic, index)

    return new Promise((resolve, reject) => {
      this.getVault().client.unlock(key, options.cas, options, async (error, data) => {
        promiseRespond(resolve, reject, error, data)
      })
    })
  }

  /**
   * Fetch a single value from a given vault (bucket)
   *
   * @param {String} topic Topic in the vault
   * @param {Object} index Index to fetch
   * @param {Object} options Fetch options
   * @param {Function} cb callback function
   * @returns {any} data
   * @memberof CouchbaseHelper
   */
  public async get(topic: string, index: archivist.IArchivistIndex, options?: any) {
    const key = this.createKey(topic, index)

    return new Promise<ICouchbaseValue>((resolve, reject) => {
      this.getVault().client.get(key, options || {}, (error, data) => {
        promiseRespond(resolve, reject, error, data)
      })
    })
  }

  /**
   * Set a single value into a given vault (bucket)
   *
   * @param {String} topic Topic in the vault
   * @param {Object} index Index to set
   * @param {*} data Data to store
   * @param {Object} options Set options
   * @returns {undefined} void
   * @memberof CouchbaseHelper
   */
  public async set(topic: string, index: archivist.IArchivistIndex, data: any, options?: couchbase.UpsertOptions) {
    const key = this.createKey(topic, index)

    return new Promise((resolve, reject) => {
      this.getVault().client.upsert(key, data, options || {}, (error) => {
        promiseRespond(resolve, reject, error, null)
      })
    })
  }

  /**
   * Add a single value into a given vault (bucket)
   *
   * @param {String} topic Topic in the vault
   * @param {Object} index Index to add
   * @param {*} data Data to store
   * @param {Object} options Add options
   * @returns {undefined} void
   * @memberof CouchbaseHelper
   */
  public async add(topic: string, index: archivist.IArchivistIndex, data: any, options?: couchbase.InsertOptions) {
    const key = this.createKey(topic, index)

    return new Promise((resolve, reject) => {
      this.getVault().client.insert(key, data, options || {}, (error) => {
        promiseRespond(resolve, reject, error, null)
      })
    })
  }

  /**
   * Replace single value into a given vault (bucket)
   *
   * @param {string} topic
   * @param {archivist.IArchivistIndex} index
   * @param {*} data
   * @param {couchbase.InsertOptions} [options]
   * @returns
   *
   * @memberof Helper
   */
  public async replace(topic: string, index: archivist.IArchivistIndex, data: any, options?: couchbase.InsertOptions) {
    const key = this.createKey(topic, index)

    return new Promise((resolve, reject) => {
      this.getVault().client.replace(key, data, options || {}, (error) => {
        promiseRespond(resolve, reject, error, null)
      })
    })
  }

  /**
   * Remove a single key from a given vault (bucket)
   *
   * @param {string} topic
   * @param {archivist.IArchivistIndex} index
   * @returns
   *
   * @memberof Helper
   */
  public async remove(topic: string, index: archivist.IArchivistIndex) {
    const key = this.createKey(topic, index)

    return new Promise((resolve, reject) => {
      this.getVault().client.remove(key, (error) => {
        promiseRespond(resolve, reject, error, null)
      })
    })
  }

  /**
   * Increment a single value from a given vault (bucket)
   *
   * @param {String} topic Topic in the vault
   * @param {Object} index Index to increment
   * @param {number} delta Increment step - may be negative
   * @param {Object} options Other options
   * @returns {number} resulting value of the increment operation
   * @memberof CouchbaseHelper
   */
  public async incr(topic: string, index: archivist.IArchivistIndex, delta: number, options?: couchbase.CounterOptions) {
    const key = this.createKey(topic, index)

    return new Promise<ICouchbaseValue>((resolve, reject) => {
      this.getVault().client.counter(key, delta, options || {}, (error, data) => {
        promiseRespond(resolve, reject, error, data)
      })
    })
  }

  /**
   * Drop all the indexes from a bucket.
   *
   * @returns {undefined} void
   * @memberof CouchbaseHelper
   */
  public async dropIndexes() {
    const query = 'SELECT RAW "DROP INDEX `" || keyspace_id || "`.`" || name' +
      ' FROM system:indexes WHERE keyspace_id = "' + this.vaultName + '"'

    const queries = await this.query(query)
    /* istanbul ignore next */
    const actions = queries.rows.map(async (q: string) => this.query(q))

    return Promise.all(actions)
  }

  /**
   * Query a Couchbase View
   *
   * @param {String} designDocName Design doc name set by archivist:migrate
   * @param {String} viewName Name of the view within the design document
   * @param {Object} options Query options
   * @returns {undefined} void
   * @memberof CouchbaseHelper
   */
  public async view(designDocName: string, viewName: string, options: any = {}) {
    const viewQuery = ViewQuery.from(designDocName, viewName).custom(options)

    // Couchbase expects the following to be stringified JSON;
    // we parse the received options object, stringify any keys
    // requiring it, and we inject the resulting value into
    // our view query
    const validOptions = ['key', 'keys', 'startkey', 'endkey']

    validOptions.forEach((key: string) => {
      if (options.hasOwnProperty(key)) {
        const fixedObj: any = {}
        fixedObj[key] = JSON.stringify(options[key])
        viewQuery.custom(fixedObj)
      }
    })

    return new Promise<IQueryResponse>((resolve, reject) => {
      this.getVault().client.query(viewQuery, (
        error: CouchbaseError,
        rows: any[], meta: couchbase.Bucket.ViewQueryResponse.Meta) => {

        promiseRespond(resolve, reject, error, { rows, meta })
      })
    })
  }
}
