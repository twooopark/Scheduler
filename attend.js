var mysql     = require('mysql'),
    db        = require("./db"),
    dateutil  = require('date-utils'),
    schedule  = require('node-schedule'); 

var scheduler_student = schedule.scheduleJob('*/5 * * * *', function(){
var query =
  'INSERT INTO `smartschool`.`attend_check`(`BLE_MAC`,`IN_TIME`,`OUT_TIME`) '+
    '(SELECT r.BLE_MAC as BLE_MAC, MIN(r.TIME) as IN_TIME, MAX(r.TIME) as OUT_TIME '+
    'FROM RAW_BLE as r, student as s '+
    'where r.CLASSROOM_MAC = s.CLASSROOM_MAC and r.ble_mac = s.ble_mac and time > current_date() '+
    'GROUP BY r.BLE_MAC) ON DUPLICATE KEY UPDATE OUT_TIME=VALUES(OUT_TIME); '

  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    }
  })


});
var attendance = schedule.scheduleJob('*/5 * * * *', function(){
var query =
	'SELECT BLE_MAC as student, DATE_FORMAT(IN_TIME, "%H:%i:%s") as inTime, DATE_FORMAT(OUT_TIME, "%H:%i:%s") as outTime FROM attend_check WHERE IN_TIME > current_date(); '
  db.query(query, (err, result) => {
    if(err) {
      console.error(query, err)
      return
    } 
    else {
    var state = [];
    var dtNow = new Date();
    var hour =  (dtNow.getHours()<10?'0':'') + dtNow.getHours() ;
    var minute = (dtNow.getMinutes()<10?'0':'') + dtNow.getMinutes();
    var second = (dtNow.getSeconds()<10?'0':'') + dtNow.getSeconds()
    var nowTime = hour + minute + second;
    var minus2Hour = nowTime-20000;


 //   console.log(nowTime + " " + minus2Hour);

	for(var i=0; i<result.length;i++){
		if(result[i].inTime.split(":").join("")>100000)
			state[i] = 0;
		else if(nowTime<150000){
			if(result[i].outTime.split(":").join("")<minus2Hour)
				state[i] = 1;
		else state[i] = 2;
		}
		else{
			if(result[i].outTime.split(":").join("")<150000)
				state[i] = 1;
			else state[i] = 2;
		}

	var query2 =
	'UPDATE `smartschool`.`attend_check` '+
	' SET state = ' + state[i] +
	' WHERE BLE_MAC = ' + result[i].student + ' AND IN_TIME > current_date(); '
	  db.query(query2, (err, result) => {
    if(err) {
      console.error(query2, err)
      return
    }
  })

	}



    }
 
});

});