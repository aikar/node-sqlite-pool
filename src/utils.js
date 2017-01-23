/**
 * SQLite client library for Node.js applications
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE.txt file in the root directory of this source tree.
 */

function prepareParams (args, requireCallback = false) {
  let callback;
  if (requireCallback) {
    if (args.length < 1) {
      throw new Error('Callback argument is required');
    }
    callback = args.pop();
  }
  const params = args.length === 1 ? args[0] : args;
  return callback ? [params, callback] : params;
}

function isThenable (obj) {
  return obj !== undefined &&
         obj !== null &&
         typeof obj === 'object' &&
         'then' in obj &&
         typeof obj.then === 'function';
}

function asyncRunner (Promise = global.Promise) {
  return function runAsync (fn, ...args) {
    const gen = fn.apply(this, args);
    return new Promise((resolve, reject) => {
      function step (key, arg) {
        let info;
        let value;

        try {
          info = gen[key](arg);
          value = info.value;
        }
        catch (error) {
          return reject(error);
        }

        if (info.done) {
          return resolve(value);
        }
        return Promise.resolve(value).then((val) => {
          step('next', val);
        }, (err) => {
          step('throw', err);
        });
      }

      return step('next');
    });
  };
}

export { prepareParams, isThenable, asyncRunner };
