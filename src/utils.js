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
  return function runAsync (fn) {
    return (...args) => {
      var gen = fn.apply(this, args);
      return new Promise((resolve, reject) => {
        function step(key, arg) {
          try {
            var info = gen[key](arg);
            var value = info.value;
          }
          catch (error) {
            reject(error);
            return;
          }

          if (info.done) {
            resolve(value);
          }
          else {
            return Promise.resolve(value).then((value) => {
              step("next", value);
            }, (err) => {
              step("throw", err);
            });
          }
        }

        return step("next");
      });
    };
  };
}

export { prepareParams, isThenable, asyncRunner };
