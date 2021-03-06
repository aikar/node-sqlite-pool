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

const fs = require('fs');
const del = require('del');
const rollup = require('rollup');
const babel = require('rollup-plugin-babel');
const pkg = require('../package.json');

// The source files to be compiled by Rollup
const files = [
  { format: 'cjs', ext: '.js' },
  { format: 'es', ext: '.es6.js' },
  { format: 'cjs', ext: '.js', presets: [['es2015', { modules: false }]], output: 'legacy' },
];

let promise = Promise.resolve();

// Clean up the output directory
promise = promise.then(() => del(['build/*']));

// Compile source code into a distributable format with Babel
for (const file of files) {
  promise = promise.then(() => rollup.rollup({
    entry: 'src/Sqlite.js',
    external: Object.keys(pkg.dependencies),
    plugins: [
      babel(Object.assign(pkg.babel, {
        babelrc: false,
        exclude: 'node_modules/**',
        presets: file.presets,
        plugins: [
          'external-helpers',
        ],
      })),
    ],
  }).then(bundle => bundle.write({
    dest: `build/${file.output || 'main'}${file.ext}`,
    format: file.format,
    sourceMap: true,
  })));
}

// Copy package.json and LICENSE.txt
promise = promise.then(() => {
  // Remove extraneous package.json properties
  delete pkg.private;
  delete pkg.devDependencies;
  delete pkg.scripts;
  delete pkg.eslintConfig;
  delete pkg.babel;

  // Rewrite entry points
  pkg.main = 'main.js';
  pkg['jsnext:main'] = 'main.es6.js';

  // Write additional files
  fs.writeFileSync('build/package.json', JSON.stringify(pkg, null, '  '), 'utf-8');
  fs.writeFileSync('build/LICENSE.txt', fs.readFileSync('LICENSE.txt', 'utf-8'), 'utf-8');
  fs.writeFileSync('build/README.md', fs.readFileSync('README.md', 'utf-8'), 'utf-8');
  fs.writeFileSync('build/API.md', fs.readFileSync('API.md', 'utf-8'), 'utf-8');
});

promise.catch(err => console.error(err.stack)); // eslint-disable-line no-console
