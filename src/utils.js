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

export { prepareParams, isThenable };
