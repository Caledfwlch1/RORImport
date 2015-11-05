// base
package main

import (
	"database/sql"
	"encoding/json"
	_ "github.com/lib/pq"
	"os"
	"strings"
	"fmt"
)

type Field struct{
	Name string
	Type string `json:",omitempty"`
}

type Mapping struct {
	Src, Dst string
	Mapped   int
	Missed   int
	Fields   map[string]Field
	Left     []Field
}

// More compact JSON serialization - can be commented

var (
	_ json.Marshaler = (*Field)(nil)
	_ json.Unmarshaler = (*Field)(nil)
)
func (f Field) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]string{f.Name,f.Type})
}
func (f *Field) UnmarshalJSON(data []byte) error {
	var arr [2]string
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	f.Name = arr[0]
	f.Type = arr[1]
	return nil
}

func fillDataBase(rec [][]string, db *sql.DB, mapStruct []Mapping) {
	var cond string
	//var res sql.Result
	dealerId := make(map[string]int)
	
	fieldsMap := map[string]string{"SV_APPT":"SA", "SV":"SV", "SL":"SL"} 

	l := len(rec)

	for i := 1; i < l; i++ {
		mapRow := make(map[string]string)
		lrec := len(rec[i])
		if lrec == 0 {
			continue
		}
		mapRow["FileType"] = rec[i][0]

		for j := 1; j < lrec; j++ {
			mapRow[rec[0][j]] = rec[i][j]
		}
		for _, el := range mapStruct {
			if el.Src == fieldsMap[mapRow["FileType"]] {
				//PrintDeb(el)
				id, _ := searchID(dealerId, mapRow["ACDealerID"], db)
				el.Fields["dealer_id"] = Field{Name:"dealer_id", Type:"integer"}
				mapRow["dealer_id"] = fmt.Sprint(id)
				//addToCustomers(el, mapRow, id)
				
				switch mapRow["FileType"] {
					case "SV":
						cond = fmt.Sprintf("dealer_id=%d and ronumber='%s'", id, mapRow["RONumber"])
						
					case "SA":
						dateTime := mapRow["AppointmentDate"] + " " + mapRow["AppointmentTime"]
						cond = fmt.Sprintf("dealer_id=%d and vin='%s' and appointment_datetime='%s'", id,
											mapRow["VehicleVIN"], dateTime)
						el.Fields["vin"] = Field{Name:"vin", Type:"character_varying(255)"}
						mapRow["vin"] = mapRow["VehicleVIN"]
						el.Fields["appointment_datetime"] = Field{Name:"appointment_datetime", Type:"timestamp_without_time_zone"}
						mapRow["appointment_datetime"] = dateTime
					case "SL":
						cond = fmt.Sprintf("dealer_id=%d and dealnumber='%s'", id, mapRow["DealNumber"])
						el.Fields["DealNumber"] = Field{Name:"DealNumber", Type:"character_varying(255)"}
					default:
						cond = ""
				}
				PrintDeb(cond)
				if cond == "" {
					continue
				}
				PrintDeb("\n", el, "\n", mapRow)
				//PrintDeb(queryStr)
				if ok, err := existRow(el.Dst, cond, db); ok {
					queryStr := makeUpdateQuery(el, mapRow, cond)
					_, err := db.Exec(queryStr) //res
					if err != nil {
						CLog.PrintLog(true, "Error execute UPDATE in ", el.Dst, ". ", err)
						continue
					}
				} else if err != nil {
					CLog.PrintLog(true, "Error execute SELECT FROM", el.Dst, ". ", err)
					continue
				} else {
					queryStr := makeInsertQuery(el, mapRow)
					PrintDeb(queryStr)
					_, err := db.Exec(queryStr)
					if err != nil {
						CLog.PrintLog(true, queryStr + "\n", "Error execute INSERT INTO ", el.Dst, ". ", err)
					}
				}


				
			}
		}
	}
	return
}

func existRow(dbName, cond string, db *sql.DB) (ok bool, err error) {
	PrintDeb(dbName, cond)
	row, err := db.Exec("SELECT id FROM " + dbName + " WHERE " + cond + ";")
	if r, err := row.RowsAffected(); r > 0 {
		return true, err
	}
	return false, err
}
/*
func addToCustomers(el Mapping, mapRow map[string]string, id int, db *sql.DB) {
	
	if ok, err := existRow("customres", fmt.Sprintf("dealer_id=%d", id), db); ok {
		
	}
	el.Fields["dealer_id"] = Field{Name:"dealer_id", Type:"integer"}
	mapRow["dealer_id"] = fmt.Sprint(id)
	
	//el.Fields["first_name"] = Field{Name:"customerfirstname", Type:"character_varying(255)"}
	//mapRow["first_name"] = mapRow["CustomerFirstName"]
	
	//el.Fields["last_name"] = Field{Name:"customerlastname", Type:"character_varying(255)"}
	//mapRow["last_name"] = mapRow["CustomerLastName"]
	
	//el.Fields["address_1"] = Field{Name:"customeraddress", Type:"character_varying(255)"}
	//mapRow["address_1"] = mapRow["CustomerAddress"]
	
	//el.Fields["city_region"] = Field{Name:"customercity", Type:"character_varying(255)"}
	//mapRow["city_region"] = mapRow["CustomerCity"]
	
	//el.Fields["state_province"] = Field{Name:"customerstate", Type:"character_varying(255)"}
	//mapRow["state_province"] = mapRow["CustomerState"]
	
	//el.Fields["postal_code"] = Field{Name:"customerzip", Type:"character_varying(255)"}
	//mapRow["postal_code"] = mapRow["CustomerZip"]
	
	//el.Fields["home_phone"] = Field{Name:"customerhomephone", Type:"character_varying(255)"}
	//mapRow["home_phone"] = mapRow["CustomerHomePhone"]
	
	//el.Fields["work_phone"] = Field{Name:"customerworkphone", Type:"character_varying(255)"}
	//mapRow["work_phone"] = mapRow["CustomerWorkPhone"]
	
	//el.Fields["cell_phone"] = Field{Name:"customercellphone", Type:"character_varying(255)"}
	//mapRow["cell_phone"] = mapRow["CustomerCellPhone"]
	
	//el.Fields["email_address_1"] = Field{Name:"customeremail", Type:"character_varying(255)"}
	//mapRow["email_address_1"] = mapRow["CustomerEmail"]
}
*/
func searchID(dealerId map[string]int, searchStr string, db *sql.DB) (id int, err error) {
	PrintDeb(dealerId, searchStr)
	if id, ok := dealerId[searchStr]; ok {
		return id, err
	}
	queryStr:= "SELECT id FROM dealers WHERE dealer_focus_id='" + searchStr + "';"
	PrintDeb(queryStr)
	err = db.QueryRow(queryStr).Scan(&id)
	PrintDeb(id)
	if err == sql.ErrNoRows {
		queryStrNew:= "INSERT INTO dealers (dealer_focus_id) VALUES ('" + searchStr + "');"
		
		if _, err := db.Exec(queryStrNew); err != nil {
			CLog.PrintLog(true, "Error INSERT INTO dealers. ", queryStrNew, " ", err)
			return 0, err
		}
		_ = db.QueryRow(queryStr).Scan(&id)
	}
	//defer rows.Close()
	dealerId[searchStr] = id
	PrintDeb(dealerId)
	return id, err
}

func makeUpdateQuery(el Mapping, val map[string]string, cond string) string {
	var timeStamp string
	if el.Src == "SV" {
		timeStamp = val["PromiseDate"] + " " + val["PromiseTime"]
		val["PromiseTime"] = timeStamp
	}
	
	queryStr := "UPDATE " + el.Dst + " set "
	for fieldFile, fieldDB := range el.Fields {
		queryStr += fieldDB.Name + "=" + normalizeValue(val[fieldFile], fieldDB.Type) + ","
	}
	if cond == "" {
		queryStr = queryStr[:len(queryStr)-1] + ";"
	} else {
		queryStr = queryStr[:len(queryStr)-1] + " WHERE "
		queryStr += cond + ";"
	}
	return queryStr
}

func makeInsertQuery(el Mapping, val map[string]string) string {
	queryStr := "INSERT INTO " + el.Dst + " ("
	addQueryStr := ") VALUES ("
	for fieldFile, fieldDB := range el.Fields { // fieldFile
		queryStr += fieldDB.Name + ","
		addQueryStr += normalizeValue(val[fieldFile], fieldDB.Type) + ","
	}
	addQueryStr = addQueryStr[:len(addQueryStr)-1] + ");"
	queryStr = queryStr[:len(queryStr)-1] + addQueryStr
	return queryStr
}

func readJSONmap(name string) (mapStruct []Mapping, err error) {
	reader, err := os.Open(name)
	if err != nil {
		CLog.PrintLog(true, "Error reading file: "+name)
		PrintDeb("Error reading file: " + name)
		return nil, err
	}
	err = json.NewDecoder(reader).Decode(&mapStruct)
	defer reader.Close()
	return mapStruct, nil
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
	quote := map[string]string{"character_varying":"'", "text":"'", "double_precision":"'", "timestamp_without_time_zone":"'",
								"integer":"'", "serial":"", "character_varying(255)":"'", "numeric":"", "time":"'"}
	switch {
		case t == "double_precision":
			if strings.TrimSpace(v) == "" {
				ret = "0"
			} else {
				i := strings.Index(v,"|")
				if i > 0 {
					ret = quote[t] + v[:i] + quote[t]
				} else {
					ret = quote[t] + v + quote[t]
				}
			}
		case t == "timestamp_without_time_zone":
			if strings.TrimSpace(v) == "" {
				ret = "'01/01/1900'"
			} else {
				ret = quote[t] + v + quote[t]
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
				ret = v
			}
		case strings.Contains(v, "'"):
			ret = strings.Replace(v, "'", "''", -1)
			ret = "'" + v + "'"
		default:
			ret = quote[t] + v + quote[t]
	}
	return ret
}

