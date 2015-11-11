// appointments
package main

import (
	"database/sql"
	"fmt"
	"strings"
)

func appointUpdate(dealer_id int, toDB map[string][]string, mapRow map[string]string, db *sql.DB) ( bool, error) {
	
	toDB["dealer_id"]	 			= []string{fmt.Sprint(dealer_id), "integer"}
	toDB["appointment_datetime"]	= []string{mapRow["AppointmentDate"] + " " + mapRow["AppointmentTime"], "timestamp_without_time_zone"}
	toDB["customer_name"] 			= []string{mapRow["CustomerName"], "character_varying(255)"}
	toDB["customer_email"] 			= []string{mapRow["CustomerEmailAddress"], "character_varying(255)"}
	toDB["service_advisor"] 		= []string{mapRow["ServiceAdvisorNumber"], "character_varying(255)"}
	toDB["vehicle_year"] 			= []string{mapRow["VehicleYear"], "character_varying(255)"}
	toDB["vehicle_make"] 			= []string{mapRow["VehicleMake"], "character_varying(255)"}
	toDB["vehicle_model"] 			= []string{mapRow["VehicleModel"], "character_varying(255)"}
	toDB["vin"] 					= []string{mapRow["VehicleVIN"], "character_varying(255)"}
	toDB["dms_appointment_number"] 	= []string{mapRow["AppointmentNumber"], "character_varying(255)"}
	toDB["dms_customer_number"] 	= []string{mapRow["CustomerNumber"], "character_varying(255)"}
	
	cond := "dealer_id=" + fmt.Sprint(dealer_id)
	if strings.TrimSpace(toDB["first_name"][0]) != "" {
		cond += " and first_name=" + normalizeValue(toDB["first_name"][0], toDB["first_name"][1])
	}
	if strings.TrimSpace(toDB["last_name"][0]) != "" {
		cond += " and last_name=" + normalizeValue(toDB["last_name"][0], toDB["last_name"][1])
	}
	cond += " and appointment_datetime=" + normalizeValue(toDB["appointment_datetime"][0], toDB["appointment_datetime"][1])
	
	return procDB("appointments", cond, toDB, db)
}

func procDB(nameDB, cond string, toDB map[string][]string, db *sql.DB) (bool, error) {
		// try update
	addqs := "SET "
	for i, j := range toDB {
		if strings.TrimSpace(j[0]) == "" {
			continue
		}
		addqs += i + "=" + normalizeValue(j[0], j[1]) + ","
	}
	addqs = addqs[:len(addqs)-1] + " WHERE " + cond + ";"
	qs := "UPDATE " + nameDB + " " + addqs

	row, err := db.Exec(qs)
	if err != nil {
		PrintDeb(qs)
		CLog.PrintLog(true, "Error UPDATE of " + nameDB + ". ", err)
		return false, err
	}
	if r, err := row.RowsAffected(); r > 0 {
		return true, err
	}
	// insert
	qs = "INSERT INTO " + nameDB + " ("
	addqs = ") VALUES ("
		
	for i, j := range toDB { 
		if strings.TrimSpace(j[0]) == "" {
			continue
		}
		qs += i + ","
		addqs += normalizeValue(j[0], j[1]) + ","
	}
	addqs = addqs[:len(addqs)-1] + ");"
	qs = qs[:len(qs)-1] + addqs
	//PrintDeb(qs)
	_, err = db.Exec(qs)
	if err != nil {
		CLog.PrintLog(true, "Error INSERT to " + nameDB + ". ", err)
		return false, err
	} 
	return false, err

}