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


  INSERT INTO `smartschool`.`student`
  (`CLASSROOM_MAC`,
  `BLE_MAC`)
  (
    select cm,sm
    from
      (
      select sm, cm, cnt,
            @s_rank := IF(@current_s = sm, @s_rank + 1, 1) AS s_rank,
            @current_s := sm
      from
        (
        select s.BLE_MAC as sm, c.MAC_DEC as cm, count(*) as cnt
        from RAW_BLE as s, classroom as c
        where s.CLASSROOM_MAC = c.MAC_DEC and( c.LOCATION like '%반' or c.LOCATION = 'TEST_BLE')
        group by s.BLE_MAC, c.MAC_DEC
        having cnt > 10
        order by BLE_MAC, cnt desc
         )as t
      )as k
    where s_rank = 1
  )
  ON DUPLICATE KEY UPDATE CLASSROOM_MAC=VALUES(CLASSROOM_MAC), BLE_MAC=VALUES(BLE_MAC); 

  INSERT INTO `smartschool`.`ble_io_update`(`BLE_MAC`,`IN_TIME`,`OUT_TIME`) 
    (SELECT r.BLE_MAC as BLE_MAC, MIN(r.TIME) as IN_TIME, MAX(r.TIME) as OUT_TIME 
    FROM RAW_BLE as r, student as s 
    where r.CLASSROOM_MAC = s.CLASSROOM_MAC and time > \''+today+'\' 
    GROUP BY r.BLE_MAC) 
  ON DUPLICATE KEY UPDATE IN_TIME=VALUES(IN_TIME), OUT_TIME=VALUES(OUT_TIME); 

  module[D865950410E3]
  237930803368163 BE
  249657935881688 LE

  module[D865950410DC]
  241961354487256 LE


  Mi-band[FBED0FEF850C]
  276995593176332 BE
  13769380982267  LE


  INSERT INTO `smartschool`.`ble_io_test`(`BLE_MAC`,`IN_TIME`,`OUT_TIME`) 
    (SELECT r.BLE_MAC as BLE_MAC, MIN(r.TIME) as IN_TIME, MAX(r.TIME) as OUT_TIME 
    FROM RAW_BLE as r, student as s 
    where r.CLASSROOM_MAC = s.CLASSROOM_MAC and time > '2017-11-23'
    GROUP BY r.BLE_MAC)
  ON DUPLICATE KEY UPDATE OUT_TIME=VALUES(OUT_TIME); 


 */