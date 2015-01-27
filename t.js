var PgDOWN = require('./index.js');
var levelup = require('levelup');

var db = levelup('pg://fritzy@localhost:5432/fritzy', {db: PgDOWN, valueEncoding: 'json'});

db.put('test2', {data: "give me a hand feller"}, function (err) {
    db.get('test2', function (err, value) {
        console.log("hello there", value);
    });
    var stream = db.createReadStream();
    stream.on('data', function (data) {
        console.log('------')
        console.log(arguments);
    });
    stream.on('end', function () {
        db.close();
    });
});
