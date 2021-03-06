/**
 * Pooled SQLite client library for Node.js
 * Based on the node-sqlite library
 *
 * Copyright © 2017 Raymond Neilson. All rights reserved.
 *
 * Original work copyright © 2016 Kriasoft, LLC. All rights reserved.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE.txt file in the root directory of this source tree.
 */

import fs from 'fs';
import path from 'path';
import EventEmitter from 'events';
import sqlite3 from 'sqlite3';
import genericPool from 'generic-pool';
import Database from './Database';
import Statement from './Statement';
import { isThenable, asyncRunner } from './utils';

// Default options
const defaults = {
  // sqlite defaults
  mode: null,
  verbose: false,
  busyTimeout: 1000,
  foreignKeys: true,
  walMode: true,
  loadExtensions: [],

  // pool defaults
  min: 1,
  max: 4,
  acquireTimeout: 1000,

  // internal defaults
  trxImmediate: true,
  delayRelease: true,

  // general defaults
  Promise: global.Promise,
};

class Sqlite extends EventEmitter {
  constructor (filename = ':memory:', options = {}) {
    super();

    // Extract options
    const {
      mode,
      verbose,
      busyTimeout,
      foreignKeys,
      walMode,
      loadExtensions,
      min,
      max,
      trxImmediate,
      delayRelease,
      acquireTimeout,
      Promise,
    } = Object.assign({}, defaults, options);

    // Re-consolidate options
    this._pool_opts = { min, max, Promise, acquireTimeoutMillis: acquireTimeout };
    this._sqlite_opts = { mode, verbose, busyTimeout, foreignKeys, walMode };
    this._sqlite_file = filename;
    this._sqlite_extn = loadExtensions;
    this._immediate = trxImmediate;
    this.delayRelease = delayRelease;
    this.Promise = Promise;

    // Async runner
    this._async = asyncRunner(Promise);

    // Special case min/max for anonymous or in-memory database
    if (filename === '' || filename === ':memory:') {
      this._pool_opts.min = 1;
      this._pool_opts.max = 1;
    }

    // Factory functions for generic-pool
    this._pool_factory = {
      create: () => this._create(),

      destroy: connection => this._destroy(connection),
    };

    // Create pool
    this._pool = genericPool.createPool(this._pool_factory, this._pool_opts);

    // Re-emit pool errors
    this._pool.on('factoryCreateError', err => this.emit('error', err));
    this._pool.on('factoryDestroyError', err => this.emit('error', err));
  }

  _create () {
    return this._async(function* _createAsync () {
      const Promise = this.Promise;
      const trxImmediate = this._immediate;
      const options = this._sqlite_opts;
      const filename = this._sqlite_file;

      // Create database connection, wait until open complete
      const connection = yield new Promise((resolve, reject) => {
        let driver;
        const callback = (err) => {
          if (err) {
            return reject(err);
          }
          return resolve(new Database(driver, { Promise, trxImmediate }));
        };

        if (options.mode !== null) {
          driver = new sqlite3.Database(filename, options.mode, callback);
        }
        else {
          driver = new sqlite3.Database(filename, callback);
        }

        // Add event re-emitters
        driver.on('error', err => this.emit('error', err));
        driver.once('open', () => this.emit('open', filename, driver));
        driver.once('close', () => this.emit('close', filename, driver));

        // Can't reset this
        if (options.verbose) {
          driver.verbose();
          driver.on('trace', (...args) => this.emit('trace', ...args));
          driver.on('profile', (...args) => this.emit('profile', ...args));
        }

        // Busy timeout default hardcoded to 1000ms, so
        // only configure if a different value given
        if (options.busyTimeout !== 1000) {
          driver.configure('busyTimeout', options.busyTimeout);
        }
      });

      // Await each for consistency
      // Load extensions
      for (const extension of this._sqlite_extn) {
        const extensionPath = path.resolve(extension);
        yield new Promise((resolve, reject) => {
          connection.driver.loadExtension(extensionPath, (err) => {
            if (err) {
              return reject(err);
            }
            return resolve();
          });
        });
      }

      // Set foreign keys and/or WAL mode as appropriate
      if (options.foreignKeys) {
        yield connection.exec('PRAGMA foreign_keys = ON;');
      }
      if (options.walMode) {
        yield connection.exec('PRAGMA journal_mode = WAL;');
      }

      // Return now-configured db connection
      return connection;
    });
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
    if (this.delayRelease) {
      return setImmediate(() => this._pool.release(connection));
    }
    return this._pool.release(connection);
  }

  _acquireRelease (fn, isAsync = false) {
    return this._async(function* _acquireReleaseAsync () {
      const connection = yield this._pool.acquire();
      let result;
      try {
        if (isAsync) {
          // Run fn as async (generator)
          result = yield this._async(fn, connection);
        }
        else {
          // Pass connection to function
          result = yield fn.call(this, connection);
        }
      }
      finally {
        this._release(connection);
      }
      return result;
    });
  }

  close () {
    return this._async(function* _close() {
      const to_close = [];
      // Queue up enough acquire() calls for all pooled connections
      while (to_close.length < this._pool.size) {
        to_close.push(this._pool.acquire().catch(() => {}));
      }
      // Now set pool to draining to block further acquisitions
      const drained = this._pool.drain();
      // Wait until acquired
      const closing = (yield Promise.all(to_close)).filter(c => !!c);
      // Close and destroy all but last connection
      closing.splice(1).forEach(conn => this._pool.destroy(conn));
      // Wait until only last connection open
      while (this._pool.size > 1) {
        yield new Promise((resolve) => setImmediate(resolve));
      }
      // Clear last connection, and let pool finish draining
      closing.forEach(conn => this._pool.destroy(conn));
      yield drained;
      yield this._pool.clear();
    });
  }

  exec (...args) {
    return this._acquireRelease(conn => conn.exec(...args)).then(() => {});
  }

  run (...args) {
    return this._acquireRelease(conn => conn.run(...args));
  }

  get (...args) {
    return this._acquireRelease(conn => conn.get(...args));
  }

  all (...args) {
    return this._acquireRelease(conn => conn.all(...args));
  }

  each (...args) {
    return this._acquireRelease(conn => conn.each(...args));
  }

  use (fn) {
    return this._acquireRelease((conn) => {
      // Pass connection to function
      const result = fn.call(this, conn);

      // If function didn't return a thenable, wait
      return isThenable(result) ? result : conn.wait().then(() => result);
    });
  }

  useAsync (gen) {
    return this._acquireRelease(gen, true);
  }

  transaction (fn, immediate = this._immediate) {
    return this._acquireRelease(conn => conn.transaction(fn, immediate));
  }

  transactionAsync (gen, immediate = this._immediate) {
    return this._acquireRelease(conn => conn.transactionAsync(gen, immediate));
  }

  /**
   * Migrates database schema to the latest version
   */
  migrate ({ force, table = 'migrations', migrationsPath = './migrations' } = {}) {
    return this._async(function* _migrateAsync () {
      const Promise = this.Promise;
      const location = path.resolve(migrationsPath);

      // Get the list of migration files, for example:
      //   { id: 1, name: 'initial', filename: '001-initial.sql' }
      //   { id: 2, name: 'feature', fielname: '002-feature.sql' }
      const migrations = yield new Promise((resolve, reject) => {
        fs.readdir(location, (err, files) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(files
              .map(x => x.match(/^(\d+).(.*?)\.sql$/))
              .filter(x => x !== null)
              .map(x => ({ id: Number(x[1]), name: x[2], filename: x[0] }))
              .sort((a, b) => (a.id > b.id ? 1 : a.id < b.id ? -1 : 0)));
          }
        });
      });

      if (!migrations.length) {
        throw new Error(`No migration files found in '${location}'.`);
      }

      // Get the list of migrations, for example:
      // { id: 1, name: 'initial', filename: '001-initial.sql', up: ..., down: ... }
      // { id: 2, name: 'feature', fielname: '002-feature.sql', up: ..., down: ... }
      yield Promise.all(migrations.map(migration => new Promise((resolve, reject) => {
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

      yield this.useAsync(function* _runMigrationsAsync (conn) {
        // Create a database table for migrations meta data if it doesn't exist
        yield conn.run(`CREATE TABLE IF NOT EXISTS "${table}" (
    id   INTEGER PRIMARY KEY,
    name TEXT    NOT NULL,
    up   TEXT    NOT NULL,
    down TEXT    NOT NULL
  )`);

        // Get the list of already applied migrations
        let dbMigrations = yield conn.all(
          `SELECT id, name, up, down FROM "${table}" ORDER BY id ASC`,
        );

        // Undo migrations that exist only in the database but not in files,
        // also undo the last migration if the `force` option was set to `last`.
        const lastMigration = migrations[migrations.length - 1];
        const prev = dbMigrations
                      .slice()
                      .sort((a, b) => (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));
        for (const migration of prev) {
          if (!migrations.some(x => x.id === migration.id) ||
              (Number.isInteger(force) && migration.id > force) ||
              (force === 'last' && migration.id === lastMigration.id)) {
            yield conn.transactionAsync(function* _downAsync (trx) {
              yield trx.exec(migration.down);
              yield trx.run(`DELETE FROM "${table}" WHERE id = ?`, migration.id);
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
        const maxMigrationId = Number.isInteger(force)
                             ? force
                             : migrations[migrations.length - 1].id;
        for (const migration of migrations) {
          if (migration.id > lastMigrationId &&
              migration.id <= maxMigrationId) {
            yield conn.transactionAsync(function* _upAsync (trx) {
              yield trx.exec(migration.up);
              yield trx.run(
                `INSERT INTO "${table}" (id, name, up, down) VALUES (?, ?, ?, ?)`,
                migration.id, migration.name, migration.up, migration.down
              );
            });
          }
        }
      });

      return this;
    });
  }

  static get OPEN_READONLY () { return sqlite3.OPEN_READONLY; }
  static get OPEN_READWRITE () { return sqlite3.OPEN_READWRITE; }
  static get OPEN_CREATE () { return sqlite3.OPEN_CREATE; }
  static get VERSION () { return sqlite3.VERSION; }
  static get SOURCE_ID () { return sqlite3.SOURCE_ID; }
  static get VERSION_NUMBER () { return sqlite3.VERSION_NUMBER; }
  static get OK () { return sqlite3.OK; }
  static get ERROR () { return sqlite3.ERROR; }
  static get INTERNAL () { return sqlite3.INTERNAL; }
  static get PERM () { return sqlite3.PERM; }
  static get ABORT () { return sqlite3.ABORT; }
  static get BUSY () { return sqlite3.BUSY; }
  static get LOCKED () { return sqlite3.LOCKED; }
  static get NOMEM () { return sqlite3.NOMEM; }
  static get READONLY () { return sqlite3.READONLY; }
  static get INTERRUPT () { return sqlite3.INTERRUPT; }
  static get IOERR () { return sqlite3.IOERR; }
  static get CORRUPT () { return sqlite3.CORRUPT; }
  static get NOTFOUND () { return sqlite3.NOTFOUND; }
  static get FULL () { return sqlite3.FULL; }
  static get CANTOPEN () { return sqlite3.CANTOPEN; }
  static get PROTOCOL () { return sqlite3.PROTOCOL; }
  static get EMPTY () { return sqlite3.EMPTY; }
  static get SCHEMA () { return sqlite3.SCHEMA; }
  static get TOOBIG () { return sqlite3.TOOBIG; }
  static get CONSTRAINT () { return sqlite3.CONSTRAINT; }
  static get MISMATCH () { return sqlite3.MISMATCH; }
  static get MISUSE () { return sqlite3.MISUSE; }
  static get NOLFS () { return sqlite3.NOLFS; }
  static get AUTH () { return sqlite3.AUTH; }
  static get FORMAT () { return sqlite3.FORMAT; }
  static get RANGE () { return sqlite3.RANGE; }
  static get NOTADB () { return sqlite3.NOTADB; }

}
Sqlite.Database = Database;
Sqlite.Statement = Statement;
export default Sqlite;
//export {Database, Statement};