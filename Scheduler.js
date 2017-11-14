var mysql = require('mysql');
var db = require("./db")
var dateutil = require('date-utils'); //UNIX TIME PARSER
var schedule = require('node-schedule'); 

/****************** scheduler ****************///출처: http://bblog.tistory.com/307 [편두리 기업블로그]
//datetime형식 >> int로 변경해야할까? http://tokyogoose.tistory.com/304
//index와 서브쿼리 oredr by를 사용해 성능 향상 http://b.fantazm.net/entry/Mysql-Query-Optimization-using-Covering-Index
//현재 임시적으로 최근 데이터 20만개를 받아오고 있으나, 20만개로 지정해두는것은 위험한일임. >> between + date를 이용한 쿼리연산으로 성능을 높이는법을 공부할 것.
//매시각 00분마다 실행하면, 쿼리문이 실행되는 동안에 생성된 데이터로 인해 예상과 다른 값이 검출되기도 한다.
//var scheduler_hourly = schedule.scheduleJob('50 * * * *', function(){ 
var scheduler_error_hourly = schedule.scheduleJob('35 * * * *', function(){      
 console.log("error_hourly Start!!")  
  var query =
  'INSERT INTO `smartschool`.`sensor_error_hourly`(`loc`,`type`,`time`,`cnt`) '+
    'SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") AS time, count(*) as cnt '+ //%h : 12시간 %H : 24시간...
    'FROM (select idx, MAC, TYPE, TIME from sensor_error order by idx desc limit 200000)as err, classroom as c, sensor as s '+
    'WHERE s.type = err.type and err.MAC = c.MAC_DEC '+
    'group by loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") '+ //시간당 최대 1만개의 데이터가 들어오는것 같다.. >> 하루 20만개 받아오게 함
  'ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);';//PK 제외
  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
      console.log("hourly Complete!!")
  })
});


/*
// v171114 . 현재 임시적으로 최근 데이터 20만개를 받아오고 있으나, 20만개로 지정해두는것은 위험한일임. >> between + date를 이용한 쿼리연산으로 성능을 높이는법을 공부할 것.
INSERT INTO `smartschool`.`sensor_error_hourly`(`loc`,`type`,`time`,`cnt`) 
  SELECT c.LOCATION as loc, s.INFO AS type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") AS time, count(*) as cnt 
  FROM (select idx, MAC, TYPE, TIME from sensor_error order by idx desc limit 200000)as err, classroom as c, sensor as s
  WHERE s.type = err.type and err.MAC = c.MAC_DEC 
  group by loc, type, DATE_FORMAT(err.TIME, "%Y-%m-%d %H") 
ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);

// v171107
INSERT INTO `smartschool`.`sensor_error_daily`(`loc`,`type`,`time`,`cnt`)
  SELECT LOCATION as loc, INFO AS type, DATE_FORMAT(TIME, "%Y-%m-%d") AS time, count(DATA) as cnt
  FROM sensor_error, classroom, sensor
  WHERE sensor.TYPE = sensor_error.TYPE and sensor_error.MAC = classroom.MAC_DEC
  group by loc, DATE_FORMAT(TIME, "%Y-%m-%d"), type
ON DUPLICATE KEY UPDATE loc=VALUES(loc), type=VALUES(type), time=VALUES(time), cnt=VALUES(cnt);
    
INSERT INTO `smartschool`.`sensor_error_hourly`(`loc`,`type`,`time`,`cnt`)
  SELECT LOCATION as loc, INFO AS type, DATE_FORMAT(TIME, "%Y-%m-%d %H") AS t, count(DATA) as cnt
  FROM sensor_error, classroom, sensor
  WHERE sensor.TYPE = sensor_error.TYPE and sensor_error.MAC = classroom.MAC_DEC
  group by loc, DATE_FORMAT(TIME, "%Y-%m-%d %H"), type 
ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);

*/
/*
//var scheduler_daily = schedule.scheduleJob('0 11 * * *', function(){ //매일 아침11시에 실행
var scheduler_data_daily = schedule.scheduleJob('00 13 * * *', function(){ 
  var query =
  'INSERT INTO `smartschool`.`sensor_error_daily`(`loc`,`type`,`time`,`cnt`) '+
    'SELECT LOCATION as loc, INFO AS type, DATE_FORMAT(TIME, "%Y-%m-%d") AS time, count(DATA) as cnt '+
    'FROM sensor_data, classroom, sensor '+
    'WHERE sensor.TYPE = sensor_data.TYPE and sensor_data.MAC = classroom.MAC_DEC '+
    //성능 향상을 위해 idx를 통해 최근 10만개의 데이터만 업데이트... 시간을 사용해 어제 날짜만 인서트 하고 싶었으나 너무 느림 테스트 결과 100개 이상의 차이가 난다.
    'and sensor_data.idx BETWEEN (select idx from sensor_data order by idx desc limit 1)-200000 and (select idx from sensor_data order by idx desc limit 1) '+
    'group by loc, DATE_FORMAT(TIME, "%Y-%m-%d"), type '+
    'ON DUPLICATE KEY UPDATE cnt=VALUES(cnt);';

  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
      console.error("daily Complete!!")
  })
});
*/
