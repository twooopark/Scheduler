var mysql     = require('mysql'),
    db        = require("./db"),
    dateutil  = require('date-utils'),
    schedule  = require('node-schedule'); 

var start_day,
    end_day,
    start_hour,
    end_hour;

var scheduler_daily = schedule.scheduleJob('01 00 * * *', function(){      
  var dt = new Date();
  end_day = dt.toFormat('YYYY-MM-DD');
  dt.setDate(dt.getDate()-1); 
  start_day = dt.toFormat('YYYY-MM-DD');

  var query =
  'INSERT INTO `smartschool`.`sensor_data_daily`(`loc`,`type`,`time`,`avge`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(d.TIME, "%Y-%m-%d") AS time, round(avg(DATA)) as avge '+
    'FROM (select idx, MAC, TYPE, DATA, TIME from sensor_data where TIME BETWEEN \''+start_day+'\' and \''+end_day+'\')as d, classroom as c, sensor as s '+
    'WHERE s.type = d.type and d.MAC = c.MAC_DEC '+
    'group by loc, type, DATE_FORMAT(d.TIME, "%Y-%m-%d") '+
  'ON DUPLICATE KEY UPDATE avge=VALUES(avge); ';
  
  query +=
  'INSERT INTO `smartschool`.`sensor_error_daily`(`loc`,`type`,`time`,`cnt`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d") AS time, count(*) as cnt '+
    'FROM (select MAC, TYPE, TIME from sensor_error WHERE TIME BETWEEN \''+start_day+'\' and \''+end_day+'\') as err, classroom as c, sensor as s '+
    'WHERE s.type = err.type and err.MAC = c.MAC_DEC '+
    'group by loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d") '+
  'ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);';




  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
      dt.setDate(dt.getDate()+1); 
      console.log("["+dt+"] : daily Complete!!")
  })
});

var scheduler_hourly = schedule.scheduleJob('00 * * * *', function(){      
  var dt = new Date();
  end_hour = dt.toFormat('YYYY-MM-DD HH24');
  dt.setHours(dt.getHours()-1)
  start_hour = dt.toFormat('YYYY-MM-DD HH24');

  var query =
  'INSERT INTO `smartschool`.`sensor_error_hourly`(`loc`,`type`,`time`,`cnt`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") AS time, count(*) as cnt '+
    'FROM (select MAC, TYPE, TIME from sensor_error WHERE TIME BETWEEN \''+start_hour+'\' and \''+end_hour+'\') as err, classroom as c, sensor as s '+
    'WHERE s.type = err.type and err.MAC = c.MAC_DEC '+
    'group by loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") '+
  'ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);';

  query +=
  'INSERT INTO `smartschool`.`sensor_data_hourly`(`loc`,`type`,`time`,`avge`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(d.TIME, "%Y-%m-%d %H") AS time, round(avg(DATA)) as avge '+
    'FROM (select idx, MAC, TYPE, DATA, TIME from sensor_data where TIME BETWEEN \''+start_hour+'\' and \''+end_hour+'\')as d, classroom as c, sensor as s '+
    'WHERE s.type = d.type and d.MAC = c.MAC_DEC '+
    'group by loc, type, DATE_FORMAT(d.TIME, "%Y-%m-%d %H") '+
  'ON DUPLICATE KEY UPDATE avge=VALUES(avge); ';

  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
      dt.setHours(dt.getHours()+1);
      console.log("["+dt+"] : hourly Complete!!")
  })
});

var scheduler_Temperature = schedule.scheduleJob('*/1 * * * *', function(){   
  var query =
  'UPDATE `smartschool`.`sensor`,( '+
    'SELECT  date_format(X.TIME, "%Y%m") ,MAX(X.DATA)as mx,MIN(X.DATA)as mn '+
    'FROM ( select * FROM sensor_data '+
            'where type = 4 '+
            'ORDER BY TIME DESC LIMIT 10000) AS X '+
    'group by date_format(X.TIME, "%Y%m"))as G '+
  'SET `MIN` = G.mn-10, `MAX` = G.mx+10 '+
  'WHERE TYPE = 4; ';

  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
  })
});