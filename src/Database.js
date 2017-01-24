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

import Statement from './Statement';
import { prepareParams, asyncRunner } from './utils';

class Database {

  /**
   * Initializes a new instance of the database client.
   * @param driver An instance of SQLite3 driver library.
   * @param promiseLibrary ES6 Promise library to use.
     */
  constructor (driver, { Promise, trxImmediate }) {
    this.driver = driver;
    this.Promise = Promise;
    this.trxImmediate = trxImmediate;
    this.async = asyncRunner(Promise);
  }

  run (sql, ...args) {
    const Promise = this.Promise;
    const params = prepareParams(args);

    return new Promise((resolve, reject) => {
      this.driver.run(sql, params, function runExecResult (err) {
        if (err) {
          reject(err);
        }
        else {
          // Per https://github.com/mapbox/node-sqlite3/wiki/API#databaserunsql-param--callback
          // when run() succeeds, the `this' object is a driver statement object. Wrap it as a
          // Statement.
          resolve(new Statement(this, Promise));
        }
      });
    });
  }

  get (sql, ...args) {
    const params = prepareParams(args);

    return new this.Promise((resolve, reject) => {
      this.driver.get(sql, params, (err, row) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(row);
        }
      });
    });
  }

  all (sql, ...args) {
    const params = prepareParams(args);

    return new this.Promise((resolve, reject) => {
      this.driver.all(sql, params, (err, rows) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(rows);
        }
      });
    });
  }

  /**
   * Runs all the SQL queries in the supplied string. No result rows are retrieved.
   */
  exec (sql) {
    return new this.Promise((resolve, reject) => {
      this.driver.exec(sql, (err) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(this);
        }
      });
    });
  }

  each (sql, ...args) {
    const [params, callback] = prepareParams(args, true);

    return new this.Promise((resolve, reject) => {
      this.driver.each(sql, params, callback, (err, rowsCount = 0) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(rowsCount);
        }
      });
    });
  }

  prepare (sql, ...args) {
    const params = prepareParams(args);

    return new this.Promise((resolve, reject) => {
      const stmt = this.driver.prepare(sql, params, (err) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(new Statement(stmt, this.Promise));
        }
      });
    });
  }

  wait () {
    return new this.Promise((resolve, reject) => {
      this.driver.wait((err) => {
        if (err) {
          reject(err);
        }
        else {
          resolve();
        }
      });
    });
  }

  transaction (fn, immediate = this.trxImmediate) {
    return this._trxWrap(fn, immediate);
  }

  transactionAsync (gen, immediate = this.trxImmediate) {
    return this._trxWrap(gen, immediate, true);
  }

  _trxWrap (fn, immediate, isAsync = false) {
    return this.async(function* _trxWrapAsync () {
      // Begin transaction
      if (immediate) {
        yield this.exec('BEGIN IMMEDIATE');
      }
      else {
        yield this.exec('BEGIN');
      }

      let result;
      try {
        // Pass connection to function
        result = yield isAsync ? this.async(fn, this) : fn.call(this, this);

        // Commit
        yield this.exec('COMMIT');
      }
      catch (err) {
        // Roll back, release connection, and re-throw
        yield this.exec('ROLLBACK');
        throw err;
      }

      return result;
    });
  }

}

export default Database;
