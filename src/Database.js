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
  constructor (driver, { Promise, trxImmediate, trxParent = null }) {
    this.driver = driver;
    this.Promise = Promise;
    this._async = asyncRunner(Promise);
    this._immediate = trxImmediate;
    this._parent = trxParent;
    this._trx = null;
  }

  run (sql, ...args) {
    this._trxCheck();
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
    this._trxCheck();
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
    this._trxCheck();
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
    this._trxCheck();

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
    this._trxCheck();
    const [params, callback] = prepareParams(args, true);

    return new this.Promise((resolve, reject) => {
      let error = null;

      const cb = (err, row) => {
        if (error !== null) {
          return;
        }
        try {
          callback(row);
        }
        catch (e) {
          error = e;
        }
      };

      const done = (err, rowsCount = 0) => {
        if (err) {
          reject(err);
        }
        else if (error) {
          reject(error);
        }
        else {
          resolve(rowsCount);
        }
      };

      this.driver.each(sql, params, cb, done);
    });
  }

  prepare (sql, ...args) {
    this._trxCheck();
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
    this._trxCheck();

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

  transaction (fn, immediate = this._immediate) {
    return this._trxWrap(fn, immediate);
  }

  transactionAsync (gen, immediate = this._immediate) {
    return this._trxWrap(gen, immediate, true);
  }

  _trxCheck (parent = false) {
    if (this._trx !== null) {
      throw new Error("A transaction is currently active for this connection");
    }
    else if (parent && this._parent !== null) {
      throw new Error("Managed savepoints are not supported at this time");
    }
  }

  _trxWrap (fn, immediate, isAsync = false) {
    this._trxCheck(true);

    return this._async(function* _trxWrapAsync () {
      // Create child Database object for transaction
      const trx = new Database(this.driver, {
        Promise: this.Promise,
        trxImmediate: this._immediate,
        trxParent: this
      });
      this._trx = trx;

      // Begin transaction
      yield immediate ? trx.exec('BEGIN IMMEDIATE') : trx.exec('BEGIN');

      let result;
      try {
        // Pass connection to function
        result = yield isAsync ? this._async(fn, trx) : fn.call(this, trx);

        // Commit
        yield trx.exec('COMMIT');
      }
      catch (err) {
        // Roll back, release connection, and re-throw
        yield trx.exec('ROLLBACK');
        throw err;
      }
      finally {
        this._trx = null;
      }

      return result;
    });
  }

}

export default Database;
