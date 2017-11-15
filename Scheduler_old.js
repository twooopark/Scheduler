var PORT = 22301;
var HOST = '0.0.0.0';
var dgram = require('dgram');
var mysql = require('mysql');
var dateutil = require('date-utils');
var schedule = require('node-schedule'); 

var db = mysql.createPool({
  host: "13.124.194.110", //no include port
  user: "dev",
  password: "xhdltmaltm",
  database: "smartschool",
  multipleStatements : true
});

var scheduler_error_hourly = schedule.scheduleJob('50 * * * *', function(){      
 console.log("error_hourly Start!!")  
  var query =
  'INSERT INTO `smartschool`.`sensor_error_hourly`(`loc`,`type`,`time`,`cnt`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") AS time, count(*) as cnt '+
    'FROM (select idx, MAC, TYPE, TIME from sensor_error order by idx desc limit 200000)as err, classroom as c, sensor as s '+
    'WHERE s.type = err.type and err.MAC = c.MAC_DEC '+
    'group by loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") '+
  'ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);';

  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
      console.log("error_hourly Complete!!")
  })
});
