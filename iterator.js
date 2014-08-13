var util = require('util');
var utils = require('./utils');
var async = require('async');
var Transform = require('stream').Transform;
var AbstractIterator = require('abstract-leveldown').AbstractIterator;
var QueryStream = require('pg-query-stream');

function PgIterator(db, options) {
    AbstractIterator.call(this, db);

    options = JSON.parse(JSON.stringify(options));

    this._bucket = db._bucketAndTable(options);
    this._reverse = !!options.reverse;
    this._keyAsBuffer = !!options.keyAsBuffer;
    this._valueAsBuffer = !!options.valueAsBuffer;
    var keyIsString = true;

    var low, high;
    var ascdesc = 'ASC';
    var lowop = '>';
    var highop = '<';

    if (options.start) {
        options.lte = options.start;
    }
    if (options.end) {
        options.gte = options.end;
    }

    if (options.hasOwnProperty('lt') && options.lt !== null && options.lt !== '') {
        highop = '<';
        high = options.lt;
    } else if (options.hasOwnProperty('lte') && options.lte !== null && options.lte !== '') {
        highop = '<=';
        high = options.lte;
    }   

    if (options.hasOwnProperty('gt') && options.gt !== null && options.gt !== '') {
        lowop = '>';
        low = options.gt;
    } else if (options.hasOwnProperty('gte') && options.gte !== null && options.gte !== '') {
        lowop = '>=';
        low = options.gte;
    }

    if (typeof high === 'undefined' || high === null || high === '') {
        high = '~';
    }

    if (typeof low === 'undefined' || low === null || low === '') {
        low = '!';
    }


    if (options.limit > 0) {
        if (options.continuation) {
            low = options.continuation;
        }
    }
    

    if (this._reverse) {
        ascdesc =  "DESC";
    }

    if (!options.limit) {
        options.limit = 0;
    }

    ///pgdown_get_range_key(tname TEXT, bucket TEXT, lowop TEXT, low TEXT, highop TEXT, high TEXT, count INTEGER, ascdesc TEXT)
    console.log(util.format('SELECT pgdown_get_range_key("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")', this._bucket.table, this._bucket.bucket, lowop, low, highop, high, options.limit, ascdesc));
    this._results = db._client.query(new QueryStream('SELECT * FROM pgdown_get_range_key($1, $2, $3, $4, $5, $6, $7, $8)', [this._bucket.table, this._bucket.bucket, lowop, low, highop, high, options.limit, ascdesc]));

    this._results.once('end', function () {
        this._endEmitted = true;
    }.bind(this));

}

util.inherits(PgIterator, AbstractIterator);

PgIterator.prototype._next = function (callback) {
    var self = this;

    var onEnd = function () {
        self._results.removeListener('readable', onReadable);
        callback();
    };

    var onReadable = function () {
        self._results.removeListener('end', onEnd);
        self._next(callback);
    };

    var obj = this._results.read();

    if (self._endEmitted) {
        callback();
    } else if (obj === null) {
        this._results.once('readable', onReadable);
        this._results.once('end', onEnd);
    } else {
        callback(null, this._keyAsBuffer ? new Buffer(obj.key) : obj.key, this._valueAsBuffer ? obj.value : obj.value.toString(), obj.extra);
    }
};

PgIterator.prototype._end = function (callback) {
    callback();
};

module.exports = PgIterator;
