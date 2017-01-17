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

import fs from 'fs';
import path from 'path';
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

    // Special case for in-memory database
    if (filename === ':memory:') {
      min = 1;
      max = 1;
    }

    // Re-consolidate options
    this._pool_opts = { min, max, Promise, acquireTimeoutMillis: acquireTimeout };
    this._sqlite_opts = { filename, mode, busyTimeout, foreignKeys, walMode };
    this._trx_immediate = trxImmediate;
    this._delay_release = delayRelease;
    this.Promise = Promise;

    // Factory functions for generic-pool
    this._pool_factory = {
      create: () => this._create(),

      destroy: (connection) => this._destroy(connection)
    };

    // Create pool
    this._pool = genericPool.createPool(this._pool_factory, this._pool_opts);
  }

  async _create () {
    // Create database connection, wait until open complete
    let connection = await new this.Promise((resolve, reject) => {
      let options = this._sqlite_opts;
      let driver;
      let callback = (err) => {
        if (err) {
          return reject(err);
        }
        return resolve(new Database(driver, { Promise: this.Promise }));
      }

      if (options.mode !== null) {
        driver = new sqlite3.Database(options.filename, options.mode, callback);
      }
      else {
        driver = new sqlite3.Database(options.filename, callback);
      }

      // Busy timeout default hardcoded to 1000ms, so
      // only configure if a different value given
      if (options.busyTimeout !== 1000) {
        driver.configure('busyTimeout', options.busyTimeout);
      }
    });

    // Set foreign keys and/or WAL mode as appropriate
    if (options.foreignKeys) {
      await connection.exec('PRAGMA foreign_keys = ON;');
    }
    if (options.walMode) {
      await connection.exec('PRAGMA journal_mode = WAL;');
    }

    // Return now-configured db connection
    return connection;
  }

  _destroy (connection) {
    return new this.Promise((resolve, reject) => {
      connection.driver.close((err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  _release (connection) {
    if (this._delay_release) {
      return setImmediate(() => this._pool.release(connection));
    }
    else {
      return this._pool.release(connection);
    }
  }

  async close () {
    await this._pool.drain();
    await this._pool.clear();
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

  /**
   * Migrates database schema to the latest version
   */
  async migrate({ force, table = 'migrations', migrationsPath = './migrations' } = {}) {
    const Promise = this.Promise;
    const location = path.resolve(migrationsPath);

    // Get the list of migration files, for example:
    //   { id: 1, name: 'initial', filename: '001-initial.sql' }
    //   { id: 2, name: 'feature', fielname: '002-feature.sql' }
    const migrations = await new Promise((resolve, reject) => {
      fs.readdir(location, (err, files) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(files
            .map(x => x.match(/^(\d+).(.*?)\.sql$/))
            .filter(x => x !== null)
            .map(x => ({ id: Number(x[1]), name: x[2], filename: x[0] }))
            .sort((a, b) => a.id > b.id));
        }
      });
    });

    if (!migrations.length) {
      throw new Error(`No migration files found in '${location}'.`);
    }

    // Get the list of migrations, for example:
    // { id: 1, name: 'initial', filename: '001-initial.sql', up: ..., down: ... }
    // { id: 2, name: 'feature', fielname: '002-feature.sql', up: ..., down: ... }
    await Promise.all(migrations.map(migration => new Promise((resolve, reject) => {
      const filename = path.join(location, migration.filename);
      fs.readFile(filename, 'utf-8', (err, data) => {
        if (err) {
          reject(err);
        }
        else {
          const [up, down] = data.split(/^--\s+?down/mi);
          if (!down) {
            reject(new Error(
              `The file ${migration.filename} is missing a '-- Down' separator.`
            ));
          }
          else {
            // Remove comments and trim whitespaces
            /* eslint-disable no-param-reassign */
            migration.up = up.replace(/^--.*?$/gm, '').trim();
            migration.down = down.replace(/^--.*?$/gm, '').trim();
            /* eslint-enable no-param-reassign */
            resolve();
          }
        }
      });
    })));

    // Create a database table for migrations meta data if it doesn't exist
    await this.run(`CREATE TABLE IF NOT EXISTS "${table}" (
  id   INTEGER PRIMARY KEY,
  name TEXT    NOT NULL,
  up   TEXT    NOT NULL,
  down TEXT    NOT NULL
)`);

    // Get the list of already applied migrations
    let dbMigrations = await this.all(
      `SELECT id, name, up, down FROM "${table}" ORDER BY id ASC`,
    );

    // Undo migrations that exist only in the database but not in files,
    // also undo the last migration if the `force` option was set to `last`.
    const lastMigration = migrations[migrations.length - 1];
    for (const migration of dbMigrations.slice().sort((a, b) => a.id < b.id)) {
      if (!migrations.some(x => x.id === migration.id) ||
          (force === 'last' && migration.id === lastMigration.id))
      {
        await this.transaction(async trx => {
          await trx.exec(migration.down);
          await trx.run(`DELETE FROM "${table}" WHERE id = ?`, migration.id);
        });
        dbMigrations = dbMigrations.filter(x => x.id !== migration.id);
      }
      else {
        break;
      }
    }

    // Apply pending migrations
    const lastMigrationId = dbMigrations.length
                          ? dbMigrations[dbMigrations.length - 1].id
                          : 0;
    for (const migration of migrations) {
      if (migration.id > lastMigrationId) {
        await this.transaction(async trx => {
          await trx.exec(migration.up);
          await trx.run(
            `INSERT INTO "${table}" (id, name, up, down) VALUES (?, ?, ?, ?)`,
            migration.id, migration.name, migration.up, migration.down,
          );
        });
      }
    }

    return this;
  }
}

export default Sqlite;
