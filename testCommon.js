var dbidx = 0;
var pg = require('pg').connect('pg://fritzy@localhost/fritzy');
var async = require('async');

exports.location = function () {
    return 'pg://fritzy@localhost/fritzy';
};

exports.lastLocation = function () {
    return 'pg://localhost/fritzy';
};

exports.cleanup = function (callback) {
    pg.getBuckets(function (err, buckets) {
        if (!buckets.buckets) buckets.buckets = [];
        async.each(buckets.buckets, function (bucket, cb) {
            if (!/^_db_test_/.test(bucket)) {
                return cb();
            }

            pg.getKeys({ bucket: bucket }, function (err, reply) {
                async.each(reply.keys ? reply.keys : [], function (key, icb) {
                    pg.del({ bucket: bucket, key: key }, icb);
                }, cb);
            });
        }, callback);
    });
};

exports.setUp = function (t) {
    pg.connect(function () {
        exports.cleanup(function (err) {
            t.notOk(err, 'cleanup returned an error');
            t.end();
        });
    });
};

exports.tearDown = function (t) {
    exports.cleanup(function (err) {
        t.notOk(err, 'cleanup returned an error');
        pg.disconnect();
        pg = require('pg').createClient();
        t.end();
    });
};

exports.collectEntries = function (iterator, callback) {
    var data = [];
    var next = function () {
        iterator.next(function (err, key, value) {
            if (err) return callback(err);
            if (!arguments.length) {
                return iterator.end(function (err) {
                    callback(err, data);
                });
            }

            data.push({ key: key, value: value });
            process.nextTick(next);
        });
    };
    next();
};
