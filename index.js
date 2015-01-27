var util = require('util');
var url = require('url');
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN;
var PgIterator = require('./iterator');
var pg = require('pg').native;
var async = require('async');

function toKey(key) {
    return typeof key === 'string' ? key : JSON.stringify(key);
}

function PgDOWN(location) {
    if (!location) {
        location = module.default_connection;
    }
    if (!(this instanceof PgDOWN)) return new PgDOWN(location);

    AbstractLevelDOWN.call(this, location);

    this._client = new pg.Client(location);
    this._bucket = 'default';
    this._location = location;
}

util.inherits(PgDOWN, AbstractLevelDOWN);

PgDOWN.destroy = function (location, callback) {
};

PgDOWN.prototype._open = function (options, callback) {
    console.log("cb", callback);
    this._client.connect(callback);
    console.log("...");
};

PgDOWN.prototype._close = function (callback) {
    this._client.end();

    process.nextTick(function () {
        callback();
    });
};

PgDOWN.prototype._bucketAndModel = function (options) {
    var bucket = options.bucket || this._bucket;
    var model;
    var out = {};
    if (Array.isArray(bucket)) {
        out.model = bucket[0];
        out.bucket = bucket[1];
    } else {
        out.model = bucket;
        out.bucket = 'default';
    }
    return out;
}

PgDOWN.prototype._put = function (key, value, options, callback) {
    var bucket = this._bucketAndModel(options);

    if (!options.indexes) {
        options.indexes = {};
    }

    var query = "SELECT pgdown_put($1, $2, $3, $4, $5)";

    this._client.query(query, [bucket.model, bucket.bucket, key, value, options.indexes], callback);

};

PgDOWN.prototype._get = function (key, options, callback) {
    var bucket = this._bucketAndModel(options);
    this._client.query("SELECT value from pgdown_keystore WHERE model=$1 AND bucket=$2 AND key=$3", [bucket.model, bucket.bucket, key], function (err, result) {
        if (result.rowCount > 0) {
            if (options.asBuffer === false) {
                callback(err, result.rows[0].value.toString());
            } else {
                callback(err, result.rows[0].value);
            }
        } else {
            callback(new Error("NotFound"));
        }
    });
};

PgDOWN.prototype._del = function (key, options, callback) {
    var bucket = this._bucketAndModel(options);
    this._client.query("DELETE FROM pgdown_keystore WHERE model=$1 AND bucket=$2 AND key=$3", [bucket.model, bucket.bucket, key], function (err, results) {
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
        }.bind(this));
    }.bind(this));
};

PgDOWN.prototype._iterator = function (options) {
    return new PgIterator(this, options);
};

module.exports = PgDOWN;
