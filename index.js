var util = require('util');
var url = require('url');
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN;
var PgIterator = require('./iterator');
var pg = require('pg');
var async = require('async');

function toKey(key) {
    return typeof key === 'string' ? key : JSON.stringify(key);
}

function PgDOWN(location) {
    if (!(this instanceof PgDOWN)) return new PgDOWN(location);

    AbstractLevelDOWN.call(this, location);

    this._client = new pg.Client(location);
    this._bucket = 'default';
}

util.inherits(PgDOWN, AbstractLevelDOWN);

PgDOWN.destroy = function (location, callback) {
    var parsed = url.parse(location);
    var client = riakpbc.createClient({ host: parsed.hostname, port: parsed.port, parse_values: false });
    var bucket = parsed.path.split('/')[1];

    client.getKeys({ bucket: bucket }, function (err, res) {
        async.each(res.keys ? res.keys : [], function (key, cb) {
            client.del({ bucket: bucket, key: key }, cb);
        }, callback);
    });
};

PgDOWN.prototype._open = function (options, callback) {
    this._client.connect(callback);
};

PgDOWN.prototype._close = function (callback) {
    this._client.end();

    process.nextTick(function () {
        callback();
    });
};

PgDOWN.prototype._bucketAndTable = function (options) {
    var bucket = options.bucket || this._bucket;
    var table;
    var out = {};
    if (Array.isArray(bucket)) {
        out.table = bucket[0];
        out.bucket = bucket[1];
    } else {
        out.table = bucket;
        out.bucket = 'default';
    }
    out.table = 'pgdown_' + out.table;
    return out;
}

PgDOWN.prototype._put = function (key, value, options, callback) {
    var bucket = this._bucketAndTable(options);

    if (!options.indexes) {
        options.indexes = {};
    }

    var query = "SELECT replace_pgdown_row2($1, $2, $3, $4, $5)";

    this._client.query(query, [bucket.table, bucket.bucket, key, value, options.indexes], callback);

};

PgDOWN.prototype._get = function (key, options, callback) {
    var bucket = this._bucketAndTable(options);
    var query = util.format("SELECT (value::text) AS value from %s WHERE bucket=$1 AND key=$2", bucket.table);
    this._client.query(query, [bucket.bucket, key], function (err, result) {
        if (result.rowCount > 0) {
            callback(err, result.rows[0].value);
        } else {
            callback("Not found");
        }
    });
};

PgDOWN.prototype._del = function (key, options, callback) {
    var bucket = this._bucketAndTalbe(options);
    this._client.query("DELETE FROM $1 WHERE bucket=$2 AND key=$3", [bucket.table, bucket.bucket, key], function (err, results) {
        callback(err);
    });
};

PgDOWN.prototype._batch = function (array, options, callback) {
    var self = this;

    this._client.query("BEGIN", function (err)  {
        async.eachSeries(array, function (item, cb) {
            if (item.type === 'put') {
                self._put(toKey(item.key), item.value, options, cb);
            } else if (item.type === 'del') {
                self._del(toKey(item.key), options, cb);
            }
        }, function (err) {
            this._client.query("COMMIT", function (err) {
                callback(err);
            });
        });
    });
};

PgDOWN.prototype._iterator = function (options) {
    return new PgIterator(this, options);
};

module.exports = PgDOWN;
