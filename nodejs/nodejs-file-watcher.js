const mongo = require('mongodb').MongoClient;
const chokidar = require('chokidar');
const path = require('path');
const fs = require('fs');
const Q = require('q');
const csv = require('csv-parser')

var log = console.log.bind(console);

var db;

var chain = Q.when();

const insertRow = function(row) {

  if (typeof db === 'undefined' || !db) {
    return log('MongoDB client undefined.');
  }

  const collection = db.collection('watchdog');

  collection.insertOne(row, function(err, res) {

    if (err) {
      return log(err);
    }
    
    return log("Record inserted:\n %s\n", JSON.stringify(row));
  });
}

const connect = async () => {
  return new Promise((resolve, reject) => {

    mongo.connect(process.env.MONGODB_URI, { useNewUrlParser: true })
    .then(client => {
      log("Connected to MongoDB.");
      db = client.db();
      resolve(db);
    })
    .catch(err => {
      log(err);
      log('MongoDB connection unsuccessful, retry after 5 seconds.');
      setTimeout(connect, 5000);
    })

  })
}

const watch = () => {

  const watcher = chokidar.watch(process.env.WATCHDOG_PATH, {
    ignored: /[\/\\]\./,
    persistent: true,
    awaitWriteFinish: {
      stabilityThreshold: 20000,
      pollInterval: 100
    },
    ignoreInitial: true
  });

  watcher.on('ready', function() {
    log('Initial scan complete. Ready for changes.'); 
  });

  watcher.on('add', function(filepath) {

    log('File', filepath, 'has been added');

    (function(filepath) {
      chain = chain.then(function() {

        return new Promise(function(resolve, reject) {

          fs.createReadStream(filepath)
            .pipe(csv({ separator: '|' }))
            .on('error', err => {
              log(err);
              reject(err)
            })
            .on('data', data => {

              // Insert data to MongoDB
              insertRow(data)
            })
            .on('end', () => {
              resolve()
            });
        });
      });
    })(filepath);
  })
}

const start = async () => {
  connect()
  .then(db => {
    log("Connected to MongoDB.");

    watch()
  })
}

start()
