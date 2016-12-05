var fs = require('fs');
var path = require('path');


// Add sql2adt.js and sql2adt.d.ts to our own node_modules folder, so it can require() itself (e.g. in tests).
fs.writeFileSync(path.join(__dirname, '../node_modules/sql2adt.js'), `module.exports = require('..');`);
fs.writeFileSync(path.join(__dirname, '../node_modules/sql2adt.d.ts'), `export * from '..';`);
