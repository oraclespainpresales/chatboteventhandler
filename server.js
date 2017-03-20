'use strict';

// Module imports
var restify = require('restify')
  , async = require('async')
  , log = require('npmlog-ts')
  , express = require('express')
  , async = require('async')
  , http = require('http')
  , bodyParser = require('body-parser')
  , _ = require('lodash')
  , alasql = require('alasql')
  , util = require('util')
  , commandLineArgs = require('command-line-args')
  , getUsage = require('command-line-usage')
;

log.timestamp = true;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  log.info("","Uncaught Exception: " + err);
  log.info("","Uncaught Exception: " + err.stack);
});
// Detect CTRL-C
process.on('SIGINT', function() {
  log.info("","Caught interrupt signal");
  log.info("","Exiting gracefully");
  process.exit(2);
});
// Main handlers registration - END

// Initialize input arguments
const optionDefinitions = [
  { name: 'dbhost', alias: 'd', type: String },
  { name: 'eventserver', alias: 's', type: String },
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'verbose', alias: 'v', type: Boolean, defaultOption: false }
];

const sections = [
  {
    header: 'IoT Racing - Chatbot Helper',
    content: 'ChatBot Helper that listens to IoT Racing events and provide a wrapper for the demo for the ChatBot'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'dbhost',
        typeLabel: '[underline]{ipaddress:port}',
        alias: 'd',
        type: String,
        description: 'DB setup server IP address/hostname and port'
      },
      {
        name: 'eventserver',
        typeLabel: '[underline]{ipaddress}',
        alias: 's',
        type: String,
        description: 'socket.io server IP address/hostname (no port is needed)'
      },
      {
        name: 'verbose',
        alias: 'v',
        description: 'Enable verbose logging.'
      },
      {
        name: 'help',
        alias: 'h',
        description: 'Print this usage guide.'
      }
    ]
  }
]
var options = undefined;

try {
  options = commandLineArgs(optionDefinitions);
} catch (e) {
  console.log(getUsage(sections));
  console.log(e.message);
  process.exit(-1);
}

if (options.help) {
  console.log(getUsage(sections));
  process.exit(0);
}

if (!options.dbhost || !options.eventserver) {
  console.log(getUsage(sections));
  process.exit(-1);
}

log.level = (options.verbose) ? 'verbose' : 'info';

// Instantiate classes & servers
var app    = express()
  , router = express.Router()
  , server = http.createServer(app)
;

// Main vars
var clients = [];
var demozones = undefined;

// REST engine initial setup
const PORT    = 3379;
const URI     = "/";
const DBURI   = '/apex/pdb1/anki/demozone/zone/'
const STATUS  = '/status/:demozone';
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(URI, router);
// REST stuff - END

// Initializing REST client BEGIN
var dbClient = restify.createJsonClient({
  url: 'https://' + options.dbhost,
  rejectUnauthorized: false,
  headers: {
    "content-type": "application/json"
  }
});
/**
var client = restify.createJsonClient({
  url: 'http://localhost:3378',
  connectTimeout: 1000,
  requestTimeout: 1000,
  retry: false,
  headers: {
    "content-type": "application/json"
  }
});
**/
// Initializing REST client END

function initializeDB(db) {
  // MAX, MIN, AVG, FASTEST LAP
  // velocidad por vuelta y coche
  db.exec('CREATE TABLE SPEED (CAR string, RACE number, LAP number, SPEED number)');
  // FASTEST LAP time, LAP# y el coche
  db.exec('CREATE TABLE LAP (CAR string, RACE number, LAP number, LAPTIME number)');
  // # offtracks per car
  db.exec('CREATE TABLE OFFTRACK (TIMESTAMP Date, CAR string, RACE number, LAP number, TRACK number)');
}

function resetDB(db) {
  db.exec('DELETE FROM SPEED');
  db.exec('DELETE FROM LAP');
  db.exec('DELETE FROM OFFTRACK');
}

function insertDataDB(db, table, data) {
  var values = "";
  data.forEach((d) => {
    values += "(";
    d.forEach((val) => {
      if (_.isString(val)) {
        values += "'" + val + "',";
      }
      if (_.isNumber(val)) {
        values += val + ",";
      }
    });
    values = values.slice(0, -1); // get rid of last useless ","
    values += "),";
  });
  values = values.slice(0, -1); // get rid of last useless ","
  var sql = util.format("INSERT INTO %s VALUES %s", table, values);
  db.exec(sql);
}

function getSpeedData(db) {
  var sql = "SELECT CAR, MAX(SPEED) AS MAXSPEED, MIN(SPEED) AS MINSPEED, AVG(SPEED) AS AVGSPEED FROM SPEED GROUP BY CAR";
  return db.exec(sql);
}

function getLapData(db) {
  var sql = "SELECT CAR, MIN(LAPTIME) AS FASTESTTIME FROM LAP GROUP BY CAR";
  return db.exec(sql);
}

function getOfftrackData(db) {
  var sql = "SELECT TIMESTAMP,CAR,LAP,TRACK FROM OFFTRACK ORDER BY TIMESTAMP,LAP,CAR";
  return db.exec(sql);
}

async.series([
    function(next) {
      dbClient.get(DBURI, function(err, req, res, obj) {
        var jBody = JSON.parse(res.body);
        if (err) {
          next(err.message);
        } else if (!jBody.items || jBody.items.length == 0) {
          next("No demozones found. Aborting.");
        } else {
          demozones = jBody.items;
          next(null);
        }
      });
    },
    function(next) {
      async.eachSeries(demozones, (demozone,callback) => {
        var d = {
          demozone: demozone.id,
          name: demozone.name,
          port: (demozone.proxyport % 100) + 10000,
          db: new alasql.Database()
        };
        initializeDB(d.db);
        var serverURI = 'http://' + options.eventserver + ':' + d.port;
        log.info(d.name, "Connecting to server at: " + serverURI);
        d.socket = require('socket.io-client')(serverURI);
        d.socket.on('connect', function() {
          log.verbose(d.name,"[EVENT] connect");
        });
        // RACE
        // [{"payload":{"data":{"data_demozone":"barcelona","raceId":11,"raceStatus":"RACING"}}}]
        // [{"payload":{"data":{"data_demozone":"barcelona","raceId":11,"raceStatus":"STOPPED"}}}]
        log.verbose(d.name, "Subscribing to namespace: " + "RACE");
        d.socket.on("race", function(msg, callback) {
          log.verbose(d.name, "RACE message received: " + JSON.stringify(msg));
          msg.forEach(function(m) {
            if (m.payload.data.raceStatus === "RACING") {
              // New race, clean existing data up
              resetDB(d.db);
            } else if (m.payload.data.raceStatus === "STOPPED") {
              // TODO: Should we empty the tables here?
            } else {
              // Should never happen
              log.error( "Unknown RACE status. Message: " + JSON.stringify(m));
            }
          });
        });
        // SPEED
        // [{"id":"3c283906-137c-4228-9568-4c9b5e9488cf","clientId":"038b6aa1-d629-45a3-b73c-95047991e732","source":"$UBSYS-2","destination":"","priority":"MEDIUM","reliability":"BEST_EFFORT","eventTime":1488750670302,"sender":"","type":"DATA","properties":{},"direction":"FROM_IOTCS","receivedTime":1488750670302,"sentTime":1488750670580,"payload":{"format":"urn:oracle:iot:anki:exploration:event:speed","data":{"data_lap":0,"data_trackid":33,"msg_source":"AAAAAATVCIIA-A4","data_datetimestring":"17/02/01 16:44:48","msg_destination":"","data_eventtime":1488750666473000000,"data_carname":"Thermo","data_datetime":0,"data_deviceid":"0000000051b9c6ae","data_raceid":17,"data_carid":"EB:EB:C4:8D:19:2D:01","data_speed":3155,"msg_priority":"MEDIUM","msg_id":"099bc141-70fd-4063-b6ea-bb4817d678d0","data_demozone":"BARCELONA","data_racestatus":"RACING","msg_sender":""}}}]
        log.verbose(d.name, "Subscribing to namespace: " + "SPEED");
        d.socket.on("speed", function(msg, callback) {
//          log.verbose(d.name, "SPEED message received: " + JSON.stringify(msg));
          var data = [];
          async.each(msg, (m, callback) => {
            // (CAR string, RACE number, LAP number, SPEED number)
            data.push([m.payload.data.data_carname, m.payload.data.data_raceid, m.payload.data.data_lap, m.payload.data.data_speed]);
            callback();
          }, (err) => {
            // All elements processed
            if (err) {
              log.error(d.name, err.message);
            } else {
              insertDataDB(d.db, 'SPEED', data);
            }
          });
        });
        // LAP
        // [{"id":"d0064fa6-8e96-44eb-b1be-a75e43d3d613","clientId":"0eda445b-8c71-4a19-b82d-98efdee5ef7e","source":"$UBSYS-2","destination":"","priority":"MEDIUM","reliability":"BEST_EFFORT","eventTime":1488750675732,"sender":"","type":"DATA","properties":{},"direction":"FROM_IOTCS","receivedTime":1488750675732,"sentTime":1488750675792,"payload":{"format":"urn:oracle:iot:anki:exploration:event:lap","data":{"data_lap":1,"msg_source":"AAAAAATVCIIA-A4","data_datetimestring":"17/02/01 16:44:56:180480","msg_destination":"","data_laptime":31440,"data_eventtime":1488750674689000000,"data_carname":"Skull","data_datetime":0,"data_deviceid":"0000000051b9c6ae","data_raceid":17,"data_carid":"F5:E0:3A:D6:41:DA:01","msg_priority":"MEDIUM","msg_id":"7dc6d885-b00f-41a0-948f-50927af4aa48","data_demozone":"BARCELONA","data_racestatus":"RACING","msg_sender":""}}}]
        log.verbose(d.name, "Subscribing to namespace: " + "LAP");
        d.socket.on("lap", function(msg, callback) {
//          log.verbose(d.name, "LAP message received: " + JSON.stringify(msg));
          var data = [];
          async.each(msg, (m, callback) => {
            // (CAR string, RACE number, LAP number, LAPTIME number)
            data.push([m.payload.data.data_carname, m.payload.data.data_raceid, m.payload.data.data_lap, m.payload.data.data_laptime]);
            callback();
          }, (err) => {
            // All elements processed
            if (err) {
              log.error(d.name, err.message);
            } else {
              insertDataDB(d.db, 'LAP', data);
            }
          });
        });
        // OFFTRACK
        // [{"id":"14a7eca4-ec90-49f4-bba1-8a7cc34b4424","clientId":"d91c6c49-4ab1-44a6-b5a2-79eab32a2f1b","source":"$UBSYS-2","destination":"","priority":"MEDIUM","reliability":"BEST_EFFORT","eventTime":1488750699751,"sender":"","type":"DATA","properties":{},"direction":"FROM_IOTCS","receivedTime":1488750699751,"sentTime":1488750699808,"payload":{"format":"urn:oracle:iot:anki:exploration:event:offtrack","data":{"data_lap":3,"data_lastknowntrack":4,"msg_source":"AAAAAATVCIIA-A4","data_datetimestring":"17/02/01 16:45:17:421189","msg_destination":"","data_message":"Off Track","data_eventtime":1488750696235000000,"data_carname":"Thermo","data_datetime":0,"data_deviceid":"0000000051b9c6ae","data_raceid":17,"data_carid":"EB:EB:C4:8D:19:2D:01","msg_priority":"HIGHEST","msg_id":"19b7ebaf-f1bc-4236-994c-ae0acd97d943","data_demozone":"BARCELONA","data_racestatus":"RACING","msg_sender":""}}}]
        log.verbose(d.name, "Subscribing to namespace: " + "OFFTRACK");
        d.socket.on("offtrack", function(msg, callback) {
//          log.verbose(d.name, "OFFTRACK message received: " + JSON.stringify(msg));
          var data = [];
          async.each(msg, (m, callback) => {
            // (TIMESTAMP Date, CAR string, RACE number, LAP number, TRACK number)
            data.push([m.payload.data.data_eventtime / 1000000, m.payload.data.data_carname, m.payload.data.data_raceid, m.payload.data.data_lap, m.payload.data.data_lastknowntrack]);
            callback();
          }, (err) => {
            // All elements processed
            if (err) {
              log.error(d.name, err.message);
            } else {
              insertDataDB(d.db, 'OFFTRACK', data);
            }
          });
        });
        clients.push(d);
        callback(null);
      }, function(err) {
        next(null);
      });
    },
    function(next) {
      router.get(STATUS, function(req, res) {
        log.verbose("", "Status request");
        var demozone = req.params.demozone;
        var client = _.find(clients, { 'demozone': demozone });
        if ( !client) {
          res.status(404).send();
        } else {
          var speedData    = getSpeedData(client.db);
          var lapData      = getLapData(client.db);
          var offtrackData = getOfftrackData(client.db);
          var data = {
            speed: speedData,
            lap: lapData,
            offtrack: offtrackData
          };
          res.status(200).send(data);
        }
      });
      server.listen(PORT, function() {
        log.info("", "REST server running on http://localhost:" + PORT + URI);
        next(null);
      });
    }
], function(err, results) {
  if (err) {
    log.error("", err.message);
    process.exit(2);
  }
});
