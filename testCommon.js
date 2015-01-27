var dbidx = 0;
var pg = require('pg')
var async = require('async');

exports.location = function () {
    return 'pg://fritzy@localhost:5432/fritzy';
};

exports.lastLocation = function () {
    return 'pg://localhost:5432/fritzy';
};

exports.cleanup = function (callback) {
    console.log("cleanup");
    callback();
};

exports.setUp = function (t) {
    pg.connect('pg://fritzy@localhost:5432/fritzy', t.end);
};

exports.tearDown = function (t) {
    t.end();
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
