var mysql     = require('mysql'),
    db        = require("./db"),
    dateutil  = require('date-utils'),
    schedule  = require('node-schedule'); 

var start_day,
    end_day,
    start_hour,
    end_hour;

var scheduler_daily = schedule.scheduleJob('05 00 * * *', function(){      
  var dt = new Date();
  end_day = dt.toFormat('YYYY-MM-DD');
  dt.setDate(dt.getDate()-1); 
  start_day = dt.toFormat('YYYY-MM-DD');

  var query =
  'INSERT INTO `smartschool`.`sensor_data_daily`(`loc`,`type`,`time`,`avge`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(d.TIME, "%Y-%m-%d") AS time, round(avg(DATA)) as avge '+
    'FROM (select idx, MAC, TYPE, DATA, TIME from sensor_data where TIME BETWEEN \''+start_day+'\' and \''+end_day+'\')as d, classroom as c, sensor as s '+
    'WHERE s.type = d.type and d.MAC = c.MAC_DEC '+
    'GROUP BY loc, type, DATE_FORMAT(d.TIME, "%Y-%m-%d") '+
  'ON DUPLICATE KEY UPDATE avge=VALUES(avge); ';
  
  query +=
  'INSERT INTO `smartschool`.`sensor_error_daily`(`loc`,`type`,`time`,`cnt`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d") AS time, count(*) as cnt '+
    'FROM (select MAC, TYPE, TIME from sensor_error WHERE TIME BETWEEN \''+start_day+'\' and \''+end_day+'\') as err, classroom as c, sensor as s '+
    'WHERE s.type = err.type and err.MAC = c.MAC_DEC '+
    'GROUP BY loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d") '+
  'ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);';
/*
  query +=
  'INSERT INTO `smartschool`.`student` '+
  '(`CLASSROOM_MAC`,`BLE_MAC`) '+
  '(SELECT cm,sm FROM '+
      '(SELECT sm, cm, cnt, @s_rank := IF(@current_s = sm, @s_rank + 1, 1) AS s_rank, @current_s := sm '+
      'FROM(SELECT s.BLE_MAC as sm, c.MAC_DEC as cm, count(*) as cnt '+
        'FROM RAW_BLE as s, classroom as c '+
        'WHERE s.CLASSROOM_MAC = c.MAC_DEC and( c.LOCATION LIKE \'%반\' or c.LOCATION = \'TEST%\') '+
        'GROUP BY s.BLE_MAC, c.MAC_DEC '+
        'HAVING cnt > 10 '+
        'ORDER BY BLE_MAC, cnt desc)as t)as k '+
    'WHERE s_rank = 1) '+
  'ON DUPLICATE KEY UPDATE CLASSROOM_MAC=VALUES(CLASSROOM_MAC), BLE_MAC=VALUES(BLE_MAC);  ';
*/

  query +=
  'INSERT INTO `smartschool`.`ble_io_test`(`BLE_MAC`,`IN_TIME`,`OUT_TIME`) '+
    '(SELECT r.BLE_MAC as BLE_MAC, MIN(r.TIME) as IN_TIME, MAX(r.TIME) as OUT_TIME '+
    'FROM RAW_BLE as r, student as s '+
    'where r.CLASSROOM_MAC = s.CLASSROOM_MAC and time > \''+start_day+'\' '+
    'GROUP BY r.BLE_MAC) '+
  'ON DUPLICATE KEY UPDATE OUT_TIME=VALUES(OUT_TIME); ';

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
    'GROUP BY loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") '+
  'ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);';

  query +=
  'INSERT INTO `smartschool`.`sensor_data_hourly`(`loc`,`type`,`time`,`avge`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(d.TIME, "%Y-%m-%d %H") AS time, round(avg(DATA)) as avge '+
    'FROM (select idx, MAC, TYPE, DATA, TIME from sensor_data where TIME BETWEEN \''+start_hour+'\' and \''+end_hour+'\')as d, classroom as c, sensor as s '+
    'WHERE s.type = d.type and d.MAC = c.MAC_DEC '+
    'GROUP BY loc, type, DATE_FORMAT(d.TIME, "%Y-%m-%d %H") '+
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

var scheduler_Temperature = schedule.scheduleJob('*/5 * * * *', function(){       
  var dt = new Date();
  var today = dt.toFormat('YYYY-MM-DD');
  
  var query =
  'UPDATE `smartschool`.`sensor`,( '+
    'SELECT  date_format(X.TIME, "%Y%m") ,MAX(X.DATA)as mx,MIN(X.DATA)as mn '+
    'FROM ( select * FROM sensor_data '+
            'where type = 4 '+
            'ORDER BY TIME DESC LIMIT 10000) AS X '+
    'GROUP BY date_format(X.TIME, "%Y%m"))as G '+
  'SET `MIN` = G.mn-10, `MAX` = G.mx+10 '+
  'WHERE TYPE = 4; ';

  query +=
  'INSERT INTO `smartschool`.`ble_io_update`(`BLE_MAC`,`IN_TIME`,`OUT_TIME`) '+
    '(SELECT r.BLE_MAC as BLE_MAC, MIN(r.TIME) as IN_TIME, MAX(r.TIME) as OUT_TIME '+
    'FROM RAW_BLE as r, student as s '+
    'where r.CLASSROOM_MAC = s.CLASSROOM_MAC and time > \''+today+'\' '+
    'GROUP BY r.BLE_MAC) '+
  'ON DUPLICATE KEY UPDATE OUT_TIME=VALUES(OUT_TIME); ';

  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
  })
});




var sendNotification = function(data) {
  var headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorization": "Basic NTlhOGM4ZWYtNWYwYy00YmI1LWI5OTktYjZjM2I5Mzc1OTBi"
  };
  
  var options = {
    host: "onesignal.com",
    port: 443,
    path: "/api/v1/notifications",
    method: "POST",
    headers: headers
  };
  
  var https = require('https');
  var req = https.request(options, function(res) {  
    res.on('data', function(data) {
      console.log("Response:");
      console.log(JSON.parse(data));
    });
  });
  
  req.on('error', function(e) {
    console.log("ERROR:");
    console.log(e);
  });
  
  req.write(JSON.stringify(data));
  req.end();
};

var message = { 
  app_id: "7e9b8655-ee5f-400b-bf5c-aaa895c6a08e",
  contents: {"en": "TEST MESSAGE"},
  included_segments: ["All"]
};


var scheduler_test = schedule.scheduleJob('59 * * * *', function(){ 
  var dt = new Date();
  var sec = dt.toFormat('YYYY-MM-DD HH24:MI:SS');
  var hour = dt.toFormat('YYYY-MM-DD HH24');
  var query = ''+
      'SELECT distinct(MAC), c.LOCATION, t.NAME '+
      'FROM smartschool.sensor_data_update as s, classroom as c left outer join teacher as t on c.MAC_DEC=t.CLASSROOM_MAC '+
      'WHERE s.MAC=c.MAC_DEC and s.TIME < \''+hour+'\' '+
      'ORDER BY c.LOCATION;';

  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    } 
    else {
      console.log("["+sec+"] : PUSH Complete!!")
      var temp = "환경센서 오류정보 ["+sec+"]\n";

      for(var i=0; i< result.length; i++){
        temp += result[i].LOCATION + ", " + (result[i].NAME = result[i].NAME == null ? "없음" : result[i].NAME) +"\n";
      }
      message.contents.ko = temp;
      sendNotification(message);
    }
  })
});