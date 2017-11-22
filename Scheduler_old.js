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

/*

  INSERT INTO `smartschool`.`sensor_data_daily`(`loc`,`type`,`time`,`avge`) 
    SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(d.TIME, "%Y-%m-%d") AS time, round(avg(DATA)) as avge 
    FROM (select idx, MAC, TYPE, DATA, TIME from sensor_data )as d, classroom as c, sensor as s 
    WHERE s.type = d.type and d.MAC = c.MAC_DEC 
    group by loc, type, DATE_FORMAT(d.TIME, "%Y-%m-%d") 
  ON DUPLICATE KEY UPDATE avge=VALUES(avge); 
  
  INSERT INTO `smartschool`.`sensor_error_daily`(`loc`,`type`,`time`,`cnt`) 
   SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d") AS time, count(*) as cnt 
    FROM (select MAC, TYPE, TIME from sensor_error) as err, classroom as c, sensor as s 
    WHERE s.type = err.type and err.MAC = c.MAC_DEC 
    group by loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d") 
  ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);

  INSERT INTO `smartschool`.`sensor_error_hourly`(`loc`,`type`,`time`,`cnt`) 
    SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") AS time, count(*) as cnt 
    FROM (select MAC, TYPE, TIME from sensor_error) as err, classroom as c, sensor as s 
    WHERE s.type = err.type and err.MAC = c.MAC_DEC 
    group by loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") 
  ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);

  INSERT INTO `smartschool`.`sensor_data_hourly`(`loc`,`type`,`time`,`avge`) 
    SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(d.TIME, "%Y-%m-%d %H") AS time, round(avg(DATA)) as avge 
    FROM (select idx, MAC, TYPE, DATA, TIME from sensor_data)as d, classroom as c, sensor as s 
    WHERE s.type = d.type and d.MAC = c.MAC_DEC 
    group by loc, type, DATE_FORMAT(d.TIME, "%Y-%m-%d %H") 
  ON DUPLICATE KEY UPDATE avge=VALUES(avge); 

  SELECT  date_format(X.TIME, "%Y%m") ,MAX(X.DATA)as mx,MIN(X.DATA)as mn 
  FROM ( select * FROM sensor_data 
        'where type = 4 
        'ORDER BY TIME DESC LIMIT 10000) AS X 
  group by date_format(X.TIME, "%Y%m"))as G 

 */