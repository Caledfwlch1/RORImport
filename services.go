package main

import (
	"database/sql"
	"fmt"
	"strings"
)

func servicesUpdate(dealer_id int, toDB map[string][]string, mapRow map[string]string, db *sql.DB) ( bool, error) {
	
	toDB["dealer_id"]	 	= []string{fmt.Sprint(dealer_id), "integer"}
	toDB["clientdealerid"] 	= []string{mapRow["ClientDealerID"], "text"}
	toDB["ronumber"] 		= []string{mapRow["RONumber"], "text"}
	toDB["rostatus"] 		= []string{mapRow["ROStatus"], "text"}
	toDB["filetype"] 		= []string{mapRow["FileType"], "text"}
	toDB["acdealerid"] 		= []string{mapRow["ACDealerID"], "text"}
	toDB["dmstype"] 		= []string{mapRow["DMSType"], "text"}
	
	toDB["opendate"] 		= []string{mapRow["OpenDate"], "timestamp_without_time_zone"}
	toDB["customernumber"] 	= []string{mapRow["CustomerNumber"], "text"}
	toDB["customername"] 	= []string{mapRow["CustomerName"], "text"}
	toDB["customerfirstname"]= toDB["first_name"]
	
	toDB["customerlastname"]= toDB["last_name"]
	
	toDB["customeraddress"] = []string{mapRow["CustomerAddress"], "text"}
	toDB["customercity"] 	= []string{mapRow["CustomerCity"], "text"}
	toDB["customerstate"] 	= []string{mapRow["CustomerState"], "text"}
	
	toDB["customerzip"] 	= []string{mapRow["CustomerZip"], "text"}
	toDB["customerhomephone"]= []string{mapRow["CustomerHomePhone"], "text"}
	toDB["customerworkphone"]= []string{mapRow["CustomerWorkPhone"], "text"}
	toDB["customercellphone"]= []string{mapRow["CustomerCellPhone"], "text"}
	toDB["customeremail"] 	= []string{mapRow["CustomerEmail"], "text"}
	toDB["customerbirthdate"]= []string{mapRow["CustomerBirthdate"], "timestamp_without_time_zone"}
	
	toDB["vehiclemileage"]	= []string{mapRow["VehicleMileage"], "integer"}
	toDB["vehicleyear"] 	= []string{mapRow["VehicleYear"], "text"}
	toDB["vehiclemake"] 	= []string{mapRow["VehicleMake"], "text"}
	toDB["vehiclemodel"] 	= []string{mapRow["VehicleModel"], "text"}
	toDB["vehiclevin"] 		= []string{mapRow["VehicleVIN"], "text"}
	toDB["serviceadvisornumber"]= []string{mapRow["ServiceAdvisorNumber"], "text"}
	
	toDB["serviceadvisorname"]= []string{mapRow["ServiceAdvisorName"], "text"}
	toDB["technicianname"] 	= []string{mapRow["TechnicianName"], "text"}
	toDB["techniciannumber"]= []string{mapRow["TechnicianNumber"], "text"}
	toDB["deliverydate"] 	= []string{mapRow["DeliveryDate"], "timestamp_without_time_zone"}
	toDB["operationcode"] 	= []string{mapRow["OperationCode"], "text"}
	toDB["operationdescription"]= []string{mapRow["OperationDescription"], "text"}
	
	toDB["roamount"] 		= []string{mapRow["ROAmount"], "double_precision"}
	toDB["warrantyname"] 	= []string{mapRow["WarrantyName"], "text"}
	toDB["warrantyexpirationdate"]= []string{mapRow["WarrantyExpirationDate"], "timestamp_without_time_zone"}
	toDB["warrantyexpirationmiles"]= []string{mapRow["WarrantyExpirationMiles"], "text"}
	toDB["salesmannumber"] 	= []string{mapRow["SalesmanNumber"], "text"}
	toDB["salesmanname"] 	= []string{mapRow["SalesmanName"], "text"}
	
	toDB["closeddate"] 		= []string{mapRow["ClosedDate"], "timestamp_without_time_zone"}
	toDB["labortypes"] 		= []string{mapRow["LaborTypes"], "text"}
	toDB["warrantylaboramount"]= []string{mapRow["WarrantyLaborAmount"], "double_precision"}
	toDB["warrantypartjobsale"]= []string{mapRow["WarrantyPartJobSale"], "double_precision"}
	toDB["warrantymiscamount"]= []string{mapRow["WarrantyMiscAmount"], "double_precision"}
	toDB["warrantyrepairordertotal"]= []string{mapRow["WarrantyRepairOrderTotal"], "double_precision"}
	
	toDB["internallaborsale"]= []string{mapRow["InternalLaborSale"], "double_precision"}
	toDB["internalpartssale"]= []string{mapRow["InternalPartsSale"], "double_precision"}
	toDB["internalmiscamount"]= []string{mapRow["InternalMiscAmount"], "double_precision"}
	toDB["internalrepairordertotal"]= []string{mapRow["InternalRepairOrderTotal"], "double_precision"}
	toDB["customerpaylaboramount"]= []string{mapRow["CustomerPayLaborAmount"], "double_precision"}
	toDB["customerpaypartssale"]= []string{mapRow["CustomerPayPartsSale"], "double_precision"}
	
	toDB["customerpaymiscsale"]= []string{mapRow["CustomerPayMiscSale"], "double_precision"}
	toDB["customerpayrepairordertotal"]= []string{mapRow["CustomerPayRepairOrderTotal"], "double_precision"}
	toDB["laborcostdollar"] = []string{mapRow["LaborCostDollar"], "double_precision"}
	toDB["partscostdollar"] = []string{mapRow["PartsCostDollar"], "double_precision"}
	toDB["misccostdollar"] 	= []string{mapRow["MiscCostDollar"], "double_precision"}
	toDB["miscdollar"] 		= []string{mapRow["MiscDollar"], "double_precision"}
	
	toDB["labordollar"] 	= []string{mapRow["LaborDollar"], "double_precision"}
	toDB["partsdollar"] 	= []string{mapRow["PartsDollar"], "double_precision"}
	toDB["vehiclecolor"] 	= []string{mapRow["VehicleColor"], "text"}
	toDB["customerpaypartscost"]= []string{mapRow["CustomerPayPartsCost"], "double_precision"}
	toDB["customerpaylaborcost"]= []string{mapRow["CustomerPayLaborCost"], "double_precision"}
	toDB["customerpaygogcost"]= []string{mapRow["CustomerPayGOGCost"], "double_precision"}
	
	toDB["customerpaysubletcost"]= []string{mapRow["CustomerPaySubletCost"], "double_precision"}
	toDB["customerpaymisccost"]= []string{mapRow["CustomerPayMiscCost"], "double_precision"}
	toDB["warrantypartscost"]= []string{mapRow["WarrantyPartsCost"], "double_precision"}
	toDB["warrantylaborcost"]= []string{mapRow["WarrantyLaborCost"], "double_precision"}
	toDB["warrantygogcost"] = []string{mapRow["WarrantyGOGCost"], "double_precision"}
	toDB["warrantysubletcost"]= []string{mapRow["WarrantySubletCost"], "double_precision"}
	
	toDB["warrantymisccost"]= []string{mapRow["WarrantyMiscCost"], "double_precision"}
	toDB["internalpartscost"]= []string{mapRow["InternalPartsCost"], "double_precision"}
	toDB["internallaborcost"]= []string{mapRow["InternalLaborCost"], "double_precision"}
	toDB["internalgogcost"] = []string{mapRow["InternalGOGCost"], "double_precision"}
	toDB["internalsubletcost"]= []string{mapRow["InternalSubletCost"], "double_precision"}
	toDB["internalmisccost"]= []string{mapRow["InternalMiscCost"], "double_precision"}
	
	toDB["totaltax"] 		= []string{mapRow["TotalTax"], "double_precision"}
	toDB["totallaborhours"] = []string{mapRow["TotalLaborHours"], "double_precision"}
	toDB["totalbillhours"] 	= []string{mapRow["TotalBillHours"], "double_precision"}
	toDB["servicecomment"] 	= []string{mapRow["ServiceComment"], "text"}
	toDB["laborcomplaint"] 	= []string{mapRow["LaborComplaint"], "text"}
	toDB["laborbillingrate"]= []string{mapRow["LaborBillingRate"], "double_precision"}
	
	toDB["labortechnicianrate"]= []string{mapRow["LaborTechnicianRate"], "double_precision"}
	toDB["appointmentflag"] = []string{mapRow["AppointmentFlag"], "text"}
	toDB["mailblock"] 		= []string{mapRow["MailBlock"], "text"}
	toDB["emailblock"] 		= []string{mapRow["EmailBlock"], "text"}
	toDB["phoneblock"] 		= []string{mapRow["PhoneBlock"], "text"}
	toDB["roinvoicedate"] 	= []string{mapRow["ROInvoiceDate"], "timestamp_without_time_zone"}

	toDB["rocustomerpaypostdate"]= []string{mapRow["ROCustomerPayPostDate"], "timestamp_without_time_zone"}
	toDB["mechanicnumber"] 	= []string{mapRow["MechanicNumber"], "text"}
	toDB["romileage"] 		= []string{mapRow["ROMileage"], "integer"}
	toDB["deliverymileage"] = []string{mapRow["DeliveryMileage"], "integer"}
	toDB["stocknumber"] 	= []string{mapRow["StockNumber"], "text"}
	toDB["recommendedservice"]= []string{mapRow["RecommendedService"], "text"}
	
	toDB["recommendations"] = []string{mapRow["Recommendations"], "text"}
	toDB["customersuffix"] 	= []string{mapRow["CustomerSuffix"], "text"}
	toDB["customersalutation"]= []string{mapRow["CustomerSalutation"], "text"}
	toDB["customeraddress2"]= []string{mapRow["CustomerAddress2"], "text"}
	toDB["customermiddlename"]= []string{mapRow["CustomerMiddleName"], "text"}
	toDB["globaloptout"] 	= []string{mapRow["GlobalOptOut"], "text"}

	toDB["promisedate"] 	= []string{mapRow["PromiseDate"], "timestamp_without_time_zone"}
	toDB["promisetime"] 	= []string{mapRow["PromiseDate"] + " " + mapRow["PromiseTime"], "timestamp_without_time_zone"}
	toDB["rologon"] 		= []string{mapRow["ROLogon"], "text"}
	toDB["labortypes2"] 	= []string{mapRow["LaborTypes2"], "text"}
	toDB["languagepreference"]= []string{mapRow["LanguagePreference"], "text"}
	toDB["misccode"] 		= []string{mapRow["MiscCode"], "text"}
	
	toDB["misccodeamount"] 	= []string{mapRow["MiscCodeAmount"], "double_precision"}
	toDB["partnumber"] 		= []string{mapRow["PartNumber"], "text"}
	toDB["partdescription"] = []string{mapRow["PartDescription"], "text"}
	toDB["partquantity"] 	= []string{mapRow["PartQuantity"], "integer"}
	toDB["misccodedescription"]= []string{mapRow["MiscCodeDescription"], "text"}
	toDB["makeprefix"] 		= []string{mapRow["MakePrefix"], "text"}
	
	toDB["department"] 		= []string{mapRow["Department"], "text"}
	toDB["rototalcost"] 	= []string{mapRow["ROTotalCost"], "double_precision"}
	toDB["pipedcomplaint"] 	= []string{mapRow["PipedComplaint"], "text"}
	toDB["pipedcomment"] 	= []string{mapRow["PipedComment"], "text"}
	toDB["mileageout"] 		= []string{mapRow["MileageOut"], "integer"}
	toDB["individualbusinessflag"]= []string{mapRow["IndividualBusinessFlag"], "text"}
	
	toDB["custgogsale"] 	= []string{mapRow["CustGOGSale"], "double_precision"}
	toDB["laborhours"] 		= []string{mapRow["LaborHours"], "double_precision"}
	toDB["billinghours"] 	= []string{mapRow["BillingHours"], "double_precision"}
	toDB["tagno"] 			= []string{mapRow["TagNo"], "text"}
	toDB["stocktype"] 		= []string{mapRow["StockType"], "text"}
	toDB["roopentime"] 		= []string{mapRow["ROOpenTime"], "text"}
	
	toDB["custsubsale"] 	= []string{mapRow["CustSUBSale"], "double_precision"}
	toDB["warrgogsale"] 	= []string{mapRow["WarrGOGSale"], "double_precision"}
	toDB["warrsubsale"] 	= []string{mapRow["WarrSUBSale"], "double_precision"}
	toDB["intlgogsale"] 	= []string{mapRow["IntlGOGSale"], "double_precision"}
	toDB["intlsubsale"] 	= []string{mapRow["IntlSUBSale"], "double_precision"}
	toDB["totalgogcost"] 	= []string{mapRow["TotalGOGCost"], "double_precision"}
	
	toDB["totalgogsale"] 	= []string{mapRow["TotalGOGSale"], "double_precision"}
	toDB["totalsubcost"] 	= []string{mapRow["TotalSUBCost"], "double_precision"}
	toDB["totalsubsale"] 	= []string{mapRow["TotalSUBSale"], "double_precision"}
	toDB["modelnum"] 		= []string{mapRow["Model#"], "text"}
	toDB["transmission"] 	= []string{mapRow["Transmission"], "text"}
	toDB["engineconfig"] 	= []string{mapRow["EngineConfig"], "text"}
	
	toDB["trimlevel"] 		= []string{mapRow["TrimLevel"], "text"}
	toDB["paymentmethod"] 	= []string{mapRow["PaymentMethod"], "text"}
	toDB["pickupdate"] 		= []string{mapRow["PickupDate"], "timestamp_without_time_zone"}
	toDB["custgender"] 		= []string{mapRow["CustGender"], "text"}
	toDB["jobstatus"] 		= []string{mapRow["JobStatus"], "text"}
	toDB["cass_std_line1"] 	= []string{mapRow["CASS_STD_LINE1"], "text"}
	
	toDB["cass_std_line2"] 	= []string{mapRow["CASS_STD_LINE2"], "text"}
	toDB["cass_std_city"] 	= []string{mapRow["CASS_STD_CITY"], "text"}
	toDB["cass_std_state"] 	= []string{mapRow["CASS_STD_STATE"], "text"}
	toDB["cass_std_zip"] 	= []string{mapRow["CASS_STD_ZIP"], "text"}
	toDB["cass_std_zip4"] 	= []string{mapRow["CASS_STD_ZIP4"], "text"}
	toDB["cass_std_dpbc"] 	= []string{mapRow["CASS_STD_DPBC"], "text"}
	
	toDB["cass_std_chkdgt"] = []string{mapRow["CASS_STD_CHKDGT"], "text"}
	toDB["cass_std_cart"] 	= []string{mapRow["CASS_STD_CART"], "text"}
	toDB["cass_std_lot"] 	= []string{mapRow["CASS_STD_LOT"], "text"}
	toDB["cass_std_lotord"] = []string{mapRow["CASS_STD_LOTORD"], "text"}
	toDB["cass_std_urb"] 	= []string{mapRow["CASS_STD_URB"], "text"}
	toDB["cass_std_fips"] 	= []string{mapRow["CASS_STD_FIPS"], "text"}
	
	toDB["cass_std_ews"] 	= []string{mapRow["CASS_STD_EWS"], "text"}
	toDB["cass_std_lacs"] 	= []string{mapRow["CASS_STD_LACS"], "text"}
	toDB["cass_std_zipmov"] = []string{mapRow["CASS_STD_ZIPMOV"], "text"}
	toDB["cass_std_z4lom"] 	= []string{mapRow["CASS_STD_Z4LOM"], "text"}
	toDB["cass_std_ndiapt"] = []string{mapRow["CASS_STD_NDIAPT"], "text"}
	toDB["cass_std_ndirr"] 	= []string{mapRow["CASS_STD_NDIRR"], "text"}
	
	toDB["cass_std_lacsrt"] = []string{mapRow["CASS_STD_LACSRT"], "text"}
	toDB["cass_std_error_cd"]= []string{mapRow["CASS_STD_ERROR_CD"], "text"}
	toDB["ncoa_ac_id"] 		= []string{mapRow["NCOA_AC_ID"], "text"}
	
	delete(toDB, "last_name")
	delete(toDB, "first_name")
	
	cond := "dealer_id=" + fmt.Sprint(dealer_id)
	if strings.TrimSpace(mapRow["ClientDealerID"]) != "" {
		cond += " and clientdealerid=" + normalizeValue(mapRow["ClientDealerID"], "text")
	}
	if strings.TrimSpace(mapRow["RONumber"]) != "" {
		cond += " and ronumber=" + normalizeValue(mapRow["RONumber"], "text")
	}
	if strings.TrimSpace(mapRow["ROStatus"]) != "" {
		cond += " and rostatus=" + normalizeValue(mapRow["ROStatus"], "text")
	}
	
	
	return procDB("services", cond, toDB, db)
}