/**
 * Pooled SQLite client library for Node.js
 *
 * Copyright © 2017 Raymond Neilson. All rights reserved.
 *
 * Some code copyright © 2016 Kriasoft, LLC. All rights reserved.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE.txt file in the root directory of this source tree.
 */

import sqlite3 from 'sqlite3';
import genericPool from 'generic-pool';
import Database from './Database';
import { isThenable } from './utils';

// Default options
const defaults = {
  // sqlite defaults
  mode: null,
  busyTimeout: 1000,
  foreignKeys: true,
  walMode: true,

  // pool defaults
  min: 1,
  max: 4,
  acquireTimeout: 1000,

  // internal defaults
  trxImmediate: true,
  delayRelease: true,

  // general defaults
  Promise: global.Promise
};

class Sqlite {
  constructor (filename, options) {
    // Extract options
    const {
      mode,
      busyTimeout,
      foreignKeys,
      walMode,
      min,
      max,
      trxImmediate,
      delayRelease,
      acquireTimeout,
      Promise
    } = Object.assign({}, defaults, options);

    // Re-consolidate options
    this._pool_opts = { min, max, Promise, acquireTimeoutMillis: acquireTimeout };
    this._sqlite_opts = { filename, mode, busyTimeout, foreignKeys, walMode };
    this._trx_immediate = trxImmediate;
    this._delay_release = delayRelease;
    this.Promise = Promise;

    // Factory functions for generic-pool
    this._pool_factory = {
      create: async () => {
        // Create database connection, wait until open complete
        let connection = await new this.Promise((resolve, reject) => {
          let opts = this._sqlite_opts;
          let driver;
          let callback = (err) => {
            if (err) {
              return reject(err);
            }
            return resolve(new Database(driver, { Promise: this.Promise }));
          }

          if (opts.mode !== null) {
            driver = new sqlite3.Database(opts.filename, opts.mode, callback);
          }
          else {
            driver = new sqlite3.Database(opts.filename, callback);
          }

          // Busy timeout default hardcoded to 1000ms, so
          // only configure if a different value given
          if (opts.busyTimeout !== 1000) {
            driver.configure('busyTimeout', opts.busyTimeout);
          }

        });

        // Set foreign keys and/or WAL mode as appropriate
        if (opts.foreignKeys) {
          await connection.exec('PRAGMA foreign_keys = ON;');
        }
        if (opts.walMode) {
          await connection.exec('PRAGMA journal_mode = WAL;');
        }

        // Return now-configured db connection
        return connection;
      },

      destroy: (connection) => {
        return new Promise((resolve, reject) => {
          connection.driver.close((err) => {
            if (err) {
              return reject(err);
            }
            return resolve();
          });
        });
      }
    };

    // Create pool
    this._pool = genericPool.createPool(this._pool_factory, this._pool_opts);
  }

  _release (connection) {
    if (this._delay_release) {
      return setImmediate(() => this._pool.release(connection));
    }
    else {
      return this._pool.release(connection);
    }
  }

  async exec (...args) {
    let connection = await this._pool.acquire();

    let result = await connection.exec(...args);

    this._release(connection);

    return result;
  }

  async run (...args) {
    let connection = await this._pool.acquire();

    let result = await connection.run(...args);

    this._release(connection);

    return result;
  }

  async get (...args) {
    let connection = await this._pool.acquire();

    let result = await connection.get(...args);

    this._release(connection);

    return result;
  }

  async all (...args) {
    let connection = await this._pool.acquire();

    let result = await connection.all(...args);

    this._release(connection);

    return result;
  }

  async each (...args) {
    let connection = await this._pool.acquire();

    let result = await connection.each(...args);

    this._release(connection);

    return result;
  }

  async transaction (fn, trx_immediate) {
    let connection = await this._pool.acquire();

    // Begin transaction
    if (trx_immediate === undefined) {
      trx_immediate = this._trx_immediate;
    }
    if (trx_immediate) {
      await connection.exec('BEGIN IMMEDIATE');
    }
    else {
      await connection.exec('BEGIN');
    }

    try {
      // Pass connection to function
      let result = fn(connection);

      // If function didn't return a thenable, wait
      if (isThenable(result)) {
        await result;
      }
      else {
        await connection.wait();
      }

      // Commit
      await connection.exec('COMMIT');
    }
    catch (err) {
      // Roll back, release connection, and re-throw
      try {
        await connection.exec('ROLLBACK');
        this._release(connection);
      }
      catch (e) {
        // In case the connection itself has a problem
        this._pool.destroy(connection);
      }
      throw err;
    }

    return result;
  }

}

export default Sqlite;
