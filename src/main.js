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
import Transaction from './Transaction';

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
      acquireTimeout,
      Promise
    } = Object.assign({}, defaults, options);

    // Re-consolidate options
    this.pool_options = { min, max, Promise, acquireTimeoutMillis: acquireTimeout };
    this.sqlite_options = { mode, busyTimeout, foreignKeys, walMode };
    this.trxImmediate = trxImmediate;
    this.Promise = Promise;

    // Factory functions for generic-pool
    this.pool_factory = {
      create: async () => {
        // Create database connection, wait until open complete
        let connection = await new Promise((resolve, reject) => {
          let driver;

          if (mode !== null) {
            driver = new sqlite3.Database(filename, mode, callback);
          }
          else {
            driver = new sqlite3.Database(filename, callback);
          }

          // Busy timeout default hardcoded to 1000ms, so
          // only configure if a different value given
          if (busyTimeout !== 1000) {
            driver.configure('busyTimeout', busyTimeout);
          }

          function callback (err) {
            if (err) {
              return reject(err);
            }
            return resolve(new Database(driver, { Promise }));
          }
        });

        // Set foreign keys and/or WAL mode as appropriate
        if (foreignKeys) {
          await connection.exec('PRAGMA foreign_keys = ON;');
        }
        if (walMode) {
          await connection.exec('PRAGMA journal_mode = WAL;');
        }

        // Return now-configured db connection
        return connection;
      },

      destroy: (connection) => connection.close()
    };

    // Create pool
    this.pool = genericPool.createPool(this.pool_factory, this.pool_options);
  }

}

export default Sqlite;
