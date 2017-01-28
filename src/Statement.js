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

import { prepareParams } from './utils';

class Statement {

  constructor (stmt, Promise) {
    this.stmt = stmt;
    this.Promise = Promise;
  }

  get sql () {
    return this.stmt.sql;
  }

  get lastID () {
    return this.stmt.lastID;
  }

  get changes () {
    return this.stmt.changes;
  }

  bind (...args) {
    const params = prepareParams(args);

    return new this.Promise((resolve, reject) => {
      this.stmt.bind(params, (err) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(this);
        }
      });
    });
  }

  reset () {
    return new this.Promise((resolve) => {
      this.stmt.reset(() => {
        resolve(this);
      });
    });
  }

  finalize () {
    return new this.Promise((resolve, reject) => {
      this.stmt.finalize((err) => {
        if (err) {
          reject(err);
        }
        else {
          resolve();
        }
      });
    });
  }

  run (...args) {
    const params = prepareParams(args);

    return new this.Promise((resolve, reject) => {
      this.stmt.run(params, (err) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(this);
        }
      });
    });
  }

  get (...args) {
    const params = prepareParams(args);

    return new this.Promise((resolve, reject) => {
      this.stmt.get(params, (err, row) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(row);
        }
      });
    });
  }

  all (...args) {
    const params = prepareParams(args);

    return new this.Promise((resolve, reject) => {
      this.stmt.all(params, (err, rows) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(rows);
        }
      });
    });
  }

  each (...args) {
    const [params, callback] = prepareParams(args, true);

    return new this.Promise((resolve, reject) => {
      let each_err = null;
      const each_fn = (err, row) => {
        if (each_err !== null) {
          return;
        }
        try {
          callback(row);
        }
        catch (e) {
          each_err = e;
        }
      };
      const done_fn = (err, rowsCount = 0) => {
        if (err) {
          reject(err);
        }
        else if (each_err) {
          reject(each_err);
        }
        else {
          resolve(rowsCount);
        }
      };
      this.stmt.each(params, each_fn, done_fn);
    });
  }

}

export default Statement;
