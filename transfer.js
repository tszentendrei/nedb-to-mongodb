#!/usr/bin/env node

var program = require('commander')
  , Nedb = require('nedb')
  , mongodb = require('mongodb')
  , fs = require('fs')
  , config = {}
  , mdb
  , ndb
  ;

// Parsing command-line options
program.version('0.1.0')
  .option('-h --mongodb-host [host]', 'Host where your MongoDB is (default: localhost)')
  .option('-p --mongodb-port [port]', 'Port on which your MongoDB server is running (default: 27017)', parseInt)
  .option('-d --mongodb-dbname [name]', 'Name of the Mongo database')
  .option('-c --mongodb-collection [name]', 'Collection to put your data into')
  .option('-n --nedb-datafile [path]', 'Path to the NeDB data file')
  .option('-k --keep-ids [true/false]', 'Whether to keep ids used by NeDB or have MongoDB generate ObjectIds (probably a good idea to use ObjectIds from now on!)')
  .parse(process.argv);


console.log("NEED SOME HELP? Type ./transfer.js --help");
console.log("-----------------------------------------");


// Making sure we have all the config parameters we need
if (!program.mongodbHost) { console.log('No MongoDB host provided, using default (localhost)'); }
config.mongodbHost = program.mongodbHost || 'localhost';

if (!program.mongodbPort) { console.log('No MongoDB port provided, using default (27017)'); }
config.mongodbPort = program.mongodbPort || 27017;

if (!program.mongodbDbname) { console.log("No MongoDB database name provided, can't proceed."); process.exit(1); }
config.mongodbDbname = program.mongodbDbname;

if (!program.mongodbCollection) { console.log("No MongoDB collection name provided, can't proceed."); process.exit(1); }
config.mongodbCollection = program.mongodbCollection;

if (!program.nedbDatafile) { console.log("No NeDB datafile path provided, can't proceed"); process.exit(1); }
config.nedbDatafile = program.nedbDatafile;

if (!program.keepIds || typeof program.keepIds !== 'string') { console.log("The --keep-ids option wasn't used or not explicitely initialized."); process.exit(1); }
config.keepIds = program.keepIds === 'true' ? true : false;

mdb = new mongodb.Db( config.mongodbDbname
                    , new mongodb.Server(config.mongodbHost, config.mongodbPort, {})
                    , { w: 1 } );

// Connect to the MongoDB database
mdb.open(function (err) {
  var collection;

  if (err) {
    console.log("Couldn't connect to the Mongo database");
    console.log(err);
    process.exit(1);
  }

  console.log("Connected to mongodb://" + config.mongodbHost + ":" + config.mongodbPort + "/" + config.mongodbDbname);

  collection = mdb.collection(config.mongodbCollection);

  fs.readFile(config.nedbDatafile, 'utf8', function(err, rawData) {
    if (err) {
      console.log("Error while loading the data from the NeDB database");
      console.log(err);
      process.exit(1);
    }
    var data = rawData.split('\n');
    if (data.length === 0) {
      console.log("The NeDB database at " + config.nedbDatafile + " contains no data, no work required");
      console.log("You should probably check the NeDB datafile path though!");
      process.exit(0);
    } else {
      console.log("Loaded data from the NeDB database at " + config.nedbDatafile + ", " + data.length + " documents");
    }
    console.log("Inserting documents (every dot represents one document) ...");
    for(var i = 0; i < data.length; i++) {
        ndb = new Nedb();
        ndb.insert(deserialize(data[i]));
        ndb.find({}, function (doc, cb) {
          process.stdout.write('.');
          if (!config.keepIds) { delete doc._id; }
          collection.insert(doc, function (err) { return cb(err); });
          }, function (err) {
          console.log("");
          if (err) {
              console.log("An error happened while inserting data");
              console.log(err);
              process.exit(1);
          }
        });
    }
    console.log("Everything went fine");
    process.exit(0);
  });
});

function deserialize (rawData) {
  return JSON.parse(rawData, function (k, v) {
    if (k === '$$date') { return new Date(v); }
    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null) { return v; }
    if (v && v.$$date) { return v.$$date; }

    return v;
  });
}