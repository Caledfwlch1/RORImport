// base
package main

import (
	"database/sql"
	//"encoding/json"
	_ "github.com/lib/pq"
	//"os"
	"strings"
	"fmt"
	"time"
)


func fillDataBase(vehicleID, custVehicleID, customerID, s3FileID, dealerID sharedMap, rec [][]string, fn string, db *sql.DB) {
	var rowsProcessed, rowsNew int
	PrintDeb("begin:", fn)
	// s3FileID := make(map[string]int)
	//mapDealerID  := make(map[string]int)
	l := len(rec)
	/*
	n, err := s3FileNumRows(s3FileID, fn, db)
	if err != nil {
		CLog.PrintLog(true, "Error search the s3file_id in s3files. ", err)
		return
	}
	
	if n == l-1 {
		CLog.PrintLog(true, "The file "+fn+" has been processed. ")
		return
	}
	*/
	if status, err := s3FileStatus(s3FileID, fn, db); status != "registered" { 
		CLog.PrintLog(true, "The file status "+fn+" is '" + status + "'. The error is ", err)
		// return  // !!!!!!!! must be !!!!!!!!!
	}

	if err := s3FileStatusUpdate(s3FileID, fn, "processing", "started_at", timeNow(), 0, 0, db); err != nil {
		CLog.PrintLog(true, "Error update the s3files to status 'processing' of file " + fn + ". ", err)
		return
	}
	
	for i := 1; i < l; i++ {
		mapRow := make(map[string]string)
		toDB := make(map[string][]string)
		lrec := len(rec[i])
		if lrec == 0 {
			continue
		}
		for j := range rec[i] {
			mapRow[rec[0][j]] = rec[i][j]
		}
		
		dealer_id, err := findDealerID(dealerID, mapRow["ACDealerID"], db)
		if err != nil {
			CLog.PrintLog(true, "Error search the dealer_id in s3files. ", err)
			return
		}
		
		toDB["first_name"] = []string{mapRow["CustomerFirstName"], "character_varying(255)"}
		toDB["last_name"] = []string{mapRow["CustomerLastName"], "character_varying(255)"}
		if toDB["first_name"][0] == "" && toDB["last_name"][0] == "" {
			name := strings.Split(mapRow["CustomerName"], ",")
			switch len(name) {
				case 1:
					toDB["first_name"] = []string{"", "character_varying(255)"}
					toDB["last_name"] = []string{name[0], "character_varying(255)"}
				case 0:
					toDB["first_name"] = []string{"", "character_varying(255)"}
					toDB["last_name"] = []string{fmt.Sprint(name), "character_varying(255)"}
				default:
					toDB["first_name"] = []string{name[1], "character_varying(255)"}
					toDB["last_name"] = []string{name[0], "character_varying(255)"}
			}
			
		}
		first_name := toDB["first_name"]
		last_name  := toDB["last_name"]
		
		toDB["home_phone"] 		= []string{mapRow["CustomerHomePhone"], "character_varying(255)"}
		toDB["work_phone"] 		= []string{mapRow["CustomerWorkPhone"], "character_varying(255)"}
		toDB["home_phone"] 		= []string{mapRow["CustomerHomePhone"], "character_varying(255)"}
		toDB["cell_phone"] 		= []string{mapRow["CustomerWorkPhone"], "character_varying(255)"}
		toDB["email_address_1"] = []string{mapRow["CustomerEmailAddress"], "character_varying(255)"}
		toDB["address_1"] 		= []string{mapRow["CustomerAddress"], "character_varying(255)"}
		toDB["city_region"] 	= []string{mapRow["CustomerCity"], "character_varying(255)"}
		toDB["state_province"] 	= []string{mapRow["CustomerState"], "character_varying(255)"}
		toDB["postal_code"] 	= []string{mapRow["CustomerZip"], "character_varying(255)"}
		toDB["dealer_id"] 		= []string{fmt.Sprint(dealer_id), "integer"}
		
		cust_id, err := customerFind(customerID, toDB, db)
		if err != nil {
			CLog.PrintLog(true, "Error SELECT/INSERT from/to customers. ", err)
			continue
		}
		toDB 				= make(map[string][]string)
		toDB["first_name"] 	= first_name
		toDB["last_name"]  	= last_name
		toDB["customer_id"]	= []string{fmt.Sprint(cust_id), "integer"}
			
		veh_id, err := vehicleFindID(vehicleID, mapRow["VehicleVIN"], db)
		if err != nil {
			CLog.PrintLog(true, "Error SELECT/INSERT from/to vehicles. ", err)
			continue
		}
		cust_veh_id, err := custVehFindID(custVehicleID, cust_id, veh_id, db)
		if err != nil {
			CLog.PrintLog(true, "Error SELECT/INSERT from/to customers_vehicles. ", err)
			continue
		}
		
		toDB["customer_vehicle_id"] 	= []string{fmt.Sprint(cust_veh_id), "integer"}
		name, _ := s3FileID.readMap(fn)
		toDB["s3file_id"] 				= []string{fmt.Sprint(name), "integer"}
		
		var ok bool
		switch mapRow["FileType"] {
			case "SV_APPT":
				ok, err = appointUpdate(dealer_id, toDB, mapRow, db)
			case "SV":
				ok, err = servicesUpdate(dealer_id, toDB, mapRow, db)
			case "SL":
				ok, err = salesUpdate(dealer_id, toDB, mapRow, db)
		}
		
		if err == nil{
			if ok {
				rowsNew++
			}
			rowsProcessed++
		}
		
		if err := s3FileStatusUpdate(s3FileID, fn, "moved", "finished_at", timeNow(), rowsNew, rowsProcessed , db); err != nil {
			CLog.PrintLog(true, "Error update the s3files to status 'moved' of file " + fn + ". ", err)
			return
		}
	}
	PrintDeb("end:", fn, ", new rows:", rowsNew, "total proc: ", rowsProcessed)
	return
}

func custVehFindID(custVehicleID sharedMap, cust_id, veh_id int, db *sql.DB) (cust_veh_id int, err error) {
	findString := fmt.Sprint(cust_id + veh_id)
	cust_veh_id, ok := custVehicleID.readMap(findString)
	if ok {
		return cust_veh_id, nil
	}
	qs := "SELECT id FROM customers_vehicles WHERE customer_id=" + fmt.Sprint(cust_id) + " and " + "vehicle_id=" + fmt.Sprint(veh_id) + ";"
	err = db.QueryRow(qs).Scan(&cust_veh_id)
	if err != nil {
		/*if err != sql.ErrNoRows {
			CLog.PrintLog(true, "Error SELECT from customers_vehicles. ", err)
			return cust_veh_id, err
		} */
		qsi := "INSERT INTO customers_vehicles (customer_id, vehicle_id) VALUES (" + fmt.Sprint(cust_id) + ", " + fmt.Sprint(veh_id) + ");"
		_, erri := db.Exec(qsi)
		if erri != nil && err != sql.ErrNoRows {
			CLog.PrintLog(true, "Error INSERT to customers_vehicles. ", "\n", err, "\n", qs, "\n", erri, "\n", qsi)
			return cust_veh_id, err
		}
	} else {
		custVehicleID.writeMap(findString, cust_veh_id)
		return cust_veh_id, err
	}
	err = db.QueryRow(qs).Scan(&cust_veh_id)
	custVehicleID.writeMap(findString, cust_veh_id)
	return cust_veh_id, err
}

func vehicleFindID(vehicleID sharedMap, vin string, db *sql.DB) (vehicle_id int, err error) {
	vehicle_id, ok := vehicleID.readMap(vin)
	if ok {
		return vehicle_id, nil
	}
	qs := "SELECT id FROM vehicles WHERE vin='" + vin + "';"
	err = db.QueryRow(qs).Scan(&vehicle_id)
	if err != nil {
		/* if err != sql.ErrNoRows {
			CLog.PrintLog(true, "Error SELECT from vehicles. ", err)
			return vehicle_id, err
		} */
		qsi := "INSERT INTO vehicles (vin) VALUES ('" + vin + "');"
		_, erri := db.Exec(qsi)
		if erri != nil && err != sql.ErrNoRows {
			CLog.PrintLog(true, "Error INSERT to vehicles. ", "\n", err, "\n", qs, "\n", erri, "\n", qsi)
			return vehicle_id, err
		}
	} else {
		vehicleID.writeMap(vin, vehicle_id)
		return vehicle_id, err
	}
	err = db.QueryRow(qs).Scan(&vehicle_id)
	vehicleID.writeMap(vin, vehicle_id)
	return vehicle_id, err
}

func customerFind(customerID sharedMap, toDB map[string][]string, db *sql.DB) (id int, err error) {
	//PrintDeb(toDB)
	qs := "SELECT id FROM customers WHERE "
	for i, j := range toDB {
		if strings.TrimSpace(j[0]) == "" {
			continue
		}
		qs += i + "=" + normalizeValue(j[0], j[1]) + " and "
	}
	qs = qs[:len(qs)-4] + ";"
	id, ok := customerID.readMap(qs)
	if ok {
		return id, nil
	}
	err = db.QueryRow(qs).Scan(&id)
	if err != nil {
		addqsi := ") VALUES ("
		qsi := "INSERT INTO customers ("
		for i, j := range toDB {
			if strings.TrimSpace(j[0]) == "" {
				continue
			}
			qsi += i + ","
			addqsi += normalizeValue(j[0], j[1]) + ","
		}
		qsi = qsi[:len(qsi)-1] + addqsi
		qsi = qsi[:len(qsi)-1] + ");"
		//PrintDeb(qs)
		_, err := db.Exec(qsi)
		if err != nil && err != sql.ErrNoRows {
			CLog.PrintLog(true, "Error INSERT to customers. ", err, "\n", qs, "\n", qsi)
			return id, err
		}
	} else {
		customerID.writeMap(qs, id)
		return id, err
	}
	//PrintDeb(qs)
	err = db.QueryRow(qs).Scan(&id)
	customerID.writeMap(qs, id)
	return id, err
}

func s3FileNumRows(s3FileID sharedMap, fn string, db *sql.DB) (num int, err error) {
	id, ok := s3FileID.readMap(fn)
	if ok {
		qs := "SELECT total_rows FROM s3files WHERE id=" + fmt.Sprint(id) + ";"
		err = db.QueryRow(qs).Scan(&num)
	} else {
		qs := "SELECT total_rows, id FROM s3files WHERE name='" + fn + "';"
		err = db.QueryRow(qs).Scan(&num, &id)
		s3FileID.writeMap(fn, id)
	}
	return num, err
}

func s3FileStatus(s3FileID sharedMap, fn string, db *sql.DB) (status string, err error) {
	id, ok := s3FileID.readMap(fn) 
	if ok {
		qs := "SELECT status FROM s3files WHERE id=" + fmt.Sprint(id) + ";"
		err = db.QueryRow(qs).Scan(&status)
	} else {
		qs := "SELECT status, id FROM s3files WHERE name='" + fn + "';"
		err = db.QueryRow(qs).Scan(&status, &id)
		s3FileID.writeMap(fn, id)
	}
	return status, err
}

func s3FileStatusUpdate(s3FileID sharedMap, fn, status, fieldname, fieldset string, rowsNew, rowsProcessed int, db *sql.DB) (err error) {
	var addset, cond string
	if fieldset != "" {
		addset = ", " + fieldname + "='" + fieldset + "'"
	}
	if rowsProcessed > 0 {
		addset += fmt.Sprintf(", new_rows=%d, total_rows=%d", rowsNew, rowsProcessed)
	}
	id, ok := s3FileID.readMap(fn) 
	if ok {
		cond = "id=" + fmt.Sprint(id)
	} else {
		cond = "name='" + fn + "'"
	}
	qs := "UPDATE s3files SET status='" + status + "'" + addset + " WHERE " + cond + ";"
	_, err = db.Exec(qs)
	return err
}

func existRow(dbName, cond string, db *sql.DB) (ok bool, err error) {
	row, err := db.Exec("SELECT id FROM " + dbName + "WHERE " + cond + ";")
	if r, err := row.RowsAffected(); r > 0 {
		return true, err
	}
	return false, err
}

func timeNow() (ts string) {
	t := time.Now()
	ts = fmt.Sprintf("%02d/%02d/%d %02d:%02d:%02d\n", t.Month(), t.Day(), t.Year(), t.Hour(), t.Minute(), t.Second())
	return ts
}


func findDealerID(dealerId sharedMap, searchStr string, db *sql.DB) (id int, err error) {
	//PrintDeb(dealerId, searchStr)
	if id, ok := dealerId.readMap(searchStr); ok {
		return id, err
	}
	queryStr:= "SELECT id FROM dealers WHERE dealer_focus_id='" + searchStr + "';"
	err = db.QueryRow(queryStr).Scan(&id)
	if err == sql.ErrNoRows {
		queryStrNew:= "INSERT INTO dealers (dealer_focus_id, created_at) VALUES ('" + searchStr + "','" + timeNow() + "');"
		
		if _, err := db.Exec(queryStrNew); err != nil {
			CLog.PrintLog(true, "Error INSERT INTO dealers. ", queryStrNew, " ", err)
			return 0, err
		}
		_ = db.QueryRow(queryStr).Scan(&id)
	}
	dealerId.writeMap(searchStr, id)

	return id, err
}

func openDB() (db *sql.DB, err error) {

	hostName, _ := Conf.GetValue("Default", "Host")
	dataBase, _ := Conf.GetValue("Default", "DataBase")
	userName, _ := Conf.GetValue("Default", "User Name")
	password, _ := Conf.GetValue("Default", "Password")

	connString := "postgres://" + userName + ":" + password + "@" + hostName + "/" + dataBase
	db, err = sql.Open("postgres", connString)
	if err != nil {
		CLog.PrintLog(true, "Error connect to database: ", dataBase, " on host:"+hostName+". ", err)
		return nil, err
	}
	if err := db.Ping(); err != nil {
		CLog.PrintLog(true, "Error connect to database: ", dataBase, " on host: "+hostName+". ", err)
		return nil, err
	}
	return db, nil
}

func normalizeValue(v, t string) (ret string) {
	//var validMiles = regexp.MustCompile("^[[:digit:]]+")
	quote := map[string]string{"character_varying":"'", "text":"'", "double_precision":"'", "timestamp_without_time_zone":"'",
								"integer":"'", "serial":"", "character_varying(255)":"'", "numeric":"", "time":"'"}
	switch {
		case t == "double_precision":
			if strings.TrimSpace(v) == "" {
				ret = "0"
			} else {
				ret = quote[t] + numericValue(v) + quote[t]
			}
		case t == "timestamp_without_time_zone":
			if strings.TrimSpace(v) == "" {
				ret = "'01/01/1900'"
			} else {
				ret = quote[t] + strings.Replace(v, ".", "/", -1) + quote[t]
			}
		case t == "time":
			if strings.TrimSpace(v) == "" {
				ret = "'00:00'"
			} else {
				ret = quote[t] + v + quote[t]
			}
		case t == "integer" || t == "numeric":
			if strings.TrimSpace(v) == "" {
				ret = "0"
			} else {
				ret = numericValue(v)
			}
		case (t == "character_varying" || t == "character_varying(255)" || t == "text") && strings.TrimSpace(v) == "":
			ret = ""
		case strings.Contains(v, "'"):
			ret = strings.Replace(v, "'", "''", -1)
			ret = "'" + ret + "'"
		default:
			ret = quote[t] + v + quote[t]
	}
	//PrintDeb(ret)
	return ret
}

func numericValue(v string) (ret string) {
	i := strings.Index(v,"|")
	if i >= 0 {
		ret = v[:i]
	} else {
		ret = v
	}
	if strings.TrimSpace(ret) == "" {
		ret = "0"
	}
	return ret
}

// pq: duplicate key value violates unique constraint
/*
func printErrSQL(err sql.Error) {
	fmt.Println("Error: Severity:", err.Severity,
    ",\n Code:", err.Code,
    ",\n Message:", err.Message,
    ",\n Detail:", err.Detail,
    ",\n Hint:", err.Hint,
    ",\n Position:", err.Position,
    ",\n InternalPosition:", err.InternalPosition,
    ",\n InternalQuery:", err.InternalQuery,
    ",\n Where:", err.Where,
    ",\n Schema:", err.Schema,
    ",\n Table:", err.Table,
    ",\n Column:", err.Column,
    ",\n DataTypeName:", err.DataTypeName,
    ",\n Constraint:", err.Constraint,
    ",\n File:", err.File,
    ",\n Line:", err.Line,
    ",\n Routine:", err.Routine)
}
*/