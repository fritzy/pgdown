var test = require('tape');
var testCommon = require('./testCommon');
var testBuffer = require('fs').readFileSync('./testdata.bin');
var PgDOWN = require('./');
PgDOWN.default_connection = "pg://fritzy@localhost:5432/fritzy";

require('abstract-leveldown/abstract/leveldown-test').args(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/open-test').args(PgDOWN, test, testCommon);
require('abstract-leveldown/abstract/open-test').open(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/put-test').all(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/del-test').all(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/get-test').all(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/put-get-del-test').all(PgDOWN, test, testCommon, testBuffer, process.browser && Uint8Array);

require('abstract-leveldown/abstract/batch-test').all(PgDOWN, test, testCommon);
require('abstract-leveldown/abstract/chained-batch-test').all(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/close-test').close(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/iterator-test').all(PgDOWN, test, testCommon);

require('abstract-leveldown/abstract/ranges-test').all(PgDOWN, test, testCommon);
