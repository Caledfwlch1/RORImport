// rorimport
package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	//"fmt"
	"github.com/jlaffaye/ftp"
	_ "github.com/lib/pq"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	bufferSize = 512000
)

var wgFile, wgFTP sync.WaitGroup

type FTPConn struct {
	ftp.ServerConn
}

// connect and login to ftp-server
func (v *FTPConn) FTPLogin(host_port, user, password string, timeout int) error {
	serconn, err := ftp.DialTimeout(host_port, time.Duration(timeout)*time.Second)
	if err != nil {
		CLog.PrintLog(true, "Error connect to ftp-server: ", host_port, ". ", err)
		return err
	}
	v.ServerConn = *serconn
	if err := v.ServerConn.Login(user, password); err != nil {
		CLog.PrintLog(true, "Error login to ftp-server for user name: ", user, ". ", err)
		return err
	}
	return nil
}

// get ftp-file and parse to lines
func (v *FTPConn) FTPFile(filename *ftp.Entry) (good bool, errOut error) {
	rd, err := v.ServerConn.Retr(filename.Name)
	if err != nil {
		errOut = err
		CLog.PrintLog(true, "Error getting the file: ", filename.Name, ". ", errOut)
		return false, errOut
	}
	reader := bufio.NewReader(rd)
	err = nil
	fiDesc, err := os.Create(filename.Name)
	if err != nil {
		errOut = err
		CLog.PrintLog(true, "Error creating the file: ", filename.Name, ". ", errOut)
		rd.Close()
		return false, errOut
	}

	err = nil
	buf := make([]byte, bufferSize)
	writer := bufio.NewWriter(fiDesc)
	writeBytes, err := io.CopyBuffer(writer, reader, buf)

	if err != nil || uint64(writeBytes) != filename.Size {
		errOut = err
		CLog.PrintLog(true, "Error writing the file: ", filename.Name, ". ", errOut)
		rd.Close()
		return false, errOut
	}
	writer.Flush()
	rd.Close()
	return true, errOut
}

func main() {

	filog := InitFunc()

	// read main configuration
	hostName, _ := Conf.GetValue("Default", "Host")
	dataBase, _ := Conf.GetValue("Default", "DataBase")
	userName, _ := Conf.GetValue("Default", "User Name")
	password, _ := Conf.GetValue("Default", "Password")
	vNumCPU, _ := Conf.Int("Default", "Quantity Proc")
	if vNumCPU <= 0 || vNumCPU > 32 {
		vNumCPU = runtime.NumCPU()
	}

	connString := "postgres://" + userName + ":" + password + "@" + hostName + "/" + dataBase
	db, err := sql.Open("postgres", connString)
	if err != nil {
		CLog.PrintLog(true, "Error connect to database: ", dataBase, " on host:"+hostName+". ", err)
		return
	}
	if err := db.Ping(); err != nil {
		CLog.PrintLog(true, "Error connect to database: ", dataBase, " on host: "+hostName+". ", err)
		return
	}

	defer db.Close()

	chFileNames := make(chan string, vNumCPU)

	t0 := time.Now()

	for i := 1; i <= vNumCPU; i++ {
		go analizeFile(chFileNames, db)
	}
	getFTPFiles(chFileNames)

	t1 := time.Now()
	PrintDeb("The call took %v to run.\n", t1.Sub(t0))
	os.Chdir("..")
	defer filog.Close()
}

func analizeFile(ch chan string, db *sql.DB) {

	for fn := range ch {
		fileDesc, err := os.Open(fn)
		if err != nil {
			CLog.PrintLog(false, "Error open file:", fn, " ", err)
			wgFTP.Done()
			continue
		}
		defer fileDesc.Close()
		ioreader := io.Reader(fileDesc)
		reader := bufio.NewReader(ioreader)
		lineReader := csv.NewReader(reader)
		lineReader.Comma = 0x09
		records, err := lineReader.ReadAll()
		if err != nil {
			CLog.PrintLog(false, "Error read from file:", fn, " ", err)
			wgFTP.Done()
			continue
		}
		l := len(records)
		if l < 2 {
			CLog.PrintLog(true, "The file ", fn, " is empty or has wrong format.")
		} else if records[0][0] != "FileType" {
			CLog.PrintLog(true, "The file ", fn, " has wrong format.")
		} else {
			fillDataBase(records, db)
		}
		wgFTP.Done()
	}
	return
}

func fillDataBase(rec [][]string, db *sql.DB) {

	sl := map[string]string{"FileType": "text", "ACDealerID": "text", "ClientDealerID": "text", "DMSType": "text", "DealNumber": "text", "CustomerNumber": "text", "CustomerName": "text", "CustomerFirstName": "text", "CustomerLastName": "text", "CustomerAddress": "text", "CustomerCity": "text", "CustomerState": "text", "CustomerZip": "text", "CustomerCounty": "text", "CustomerHomePhone": "text", "CustomerWorkPhone": "text", "CustomerCellPhone": "text", "CustomerPagerPhone": "text", "CustomerEmail": "text", "CustomerBirthDate": "text", "MailBlock": "text", "CoBuyerName": "text", "CoBuyerFirstName": "text", "CoBuyerLastName": "text", "CoBuyerAddress": "text", "CoBuyerCity": "text", "CoBuyerState": "text", "CoBuyerZip": "text", "CoBuyerCounty": "text", "CoBuyerHomePhone": "text", "CoBuyerWorkPhone": "text", "CoBuyerBirthDate": "text", "Salesman_1_Number": "text", "Salesman_1_Name": "text", "Salesman_2_Number": "text", "Salesman_2_Name": "text", "ClosingManagerName": "text", "ClosingManagerNumber": "text", "F_AND_I_ManagerNumber": "text", "F_AND_I_ManagerName": "text", "SalesManagerNumber": "text", "SalesManagerName": "text", "EntryDate": "text", "DealBookDate": "text", "VehicleYear": "text", "VehicleMake": "text", "VehicleModel": "text", "VehicleStockNumber": "text", "VehicleVIN": "text", "VehicleExteriorColor": "text", "VehicleInteriorColor": "text", "VehicleMileage": "text", "VehicleType": "text", "InServiceDate": "text", "HoldBackAmount": "text", "DealType": "text", "SaleType": "text", "BankCode": "text", "BankName": "text", "SalesmanCommission": "text", "GrossProfitSale": "text", "FinanceReserve": "text", "CreditLifePremium": "text", "CreditLifeCommision": "text", "TotalInsuranceReserve": "text", "BalloonAmount": "text", "CashPrice": "text", "AmountFinanced": "text", "TotalOfPayments": "text", "MSRP": "text", "DownPayment": "text", "SecurityDesposit": "text", "Rebate": "text", "Term": "text", "RetailPayment": "text", "PaymentType": "text", "RetailFirstPayDate": "text", "LeaseFirstPayDate": "text", "DayToFirstPayment": "text", "LeaseAnnualMiles": "text", "MileageRate": "text", "APRRate": "text", "ResidualAmount": "text", "LicenseFee": "text", "RegistrationFee": "text", "TotalTax": "text", "ExtendedWarrantyName": "text", "ExtendedWarrantyTerm": "text", "ExtendedWarrantyLimitMiles": "text", "ExtendedWarrantyDollar": "text", "ExtendedWarrantyProfit": "text", "FrontGross": "text", "BackGross": "text", "TradeIn_1_VIN": "text", "TradeIn_2_VIN": "text", "TradeIn_1_Make": "text", "TradeIn_2_Make": "text", "TradeIn_1_Model": "text", "TradeIn_2_Model": "text", "TradeIn_1_ExteriorColor": "text", "TradeIn_2_ExteriorColor": "text", "TradeIn_1_Year": "text", "TradeIn_2_Year": "text", "TradeIn_1_Mileage": "text", "TradeIn_2_Mileage": "text", "TradeIn_1_Gross": "text", "TradeIn_2_Gross": "text", "TradeIn_1_Payoff": "text", "TradeIn_2_Payoff": "text", "TradeIn_1_ACV": "text", "TradeIn_2_ACV": "text", "Fee_1_Name": "text", "Fee_1_Fee": "text", "Fee_1_Commission": "text", "Fee_2_Name": "text", "Fee_2_Fee": "text", "Fee_2_Commission": "text", "Fee_3_Name": "text", "Fee_3_Fee": "text", "Fee_3_Commission": "text", "Fee_4_Name": "text", "Fee_4_Fee": "text", "Fee_4_Commission": "text", "Fee_5_Name": "text", "Fee_5_Fee": "text", "Fee_5_Commission": "text", "Fee_6_Name": "text", "Fee_6_Fee": "text", "Fee_6_Commission": "text", "Fee_7_Name": "text", "Fee_7_Fee": "text", "Fee_7_Commission": "text", "Fee_8_Name": "text", "Fee_8_Fee": "text", "Fee_8_Commission": "text", "Fee_9_Name": "text", "Fee_9_Fee": "text", "Fee_9_Commission": "text", "Fee_10_Name": "text", "Fee_10_Fee": "text", "Fee_10_Commission": "text", "ContractDate": "text", "InsuranceName": "text", "InsuranceAgentName": "text", "InsuranceAddress": "text", "InsuranceCity": "text", "InsuranceState": "text", "InsuranceZip": "text", "InsurancePhone": "text", "InsurancePolicyNumber": "text", "InsuranceEffectiveDate": "text", "InsuranceExpirationDate": "text", "InsuranceCompensationDeduction": "text", "TradeIn_1_InteriorColor": "text", "TradeIn_2_InteriorColor": "text", "PhoneBlock": "text", "LicensePlateNumber": "text", "Cost": "text", "InvoiceAmount": "text", "FinanceCharge": "text", "TotalPickupPayment": "text", "TotalAccessories": "text", "TotalDriveOffAmount": "text", "EmailBlock": "text", "ModelDescriptionOfCarSold": "text", "VehicleClassification": "text", "ModelNumberOfCarSold": "text", "GAPPremium": "text", "LastInstallmentDate": "text", "CashDeposit": "text", "AHPremium": "text", "LeaseRate": "text", "DealerSelect": "text", "LeasePayment": "text", "LeaseNetCapCost": "text", "LeaseTotalCapReduction": "text", "DealStatus": "text", "CustomerSuffix": "text", "CustomerSalutation": "text", "CustomerAddress2": "text", "CustomerMiddleName": "text", "GlobalOptOut": "text", "LeaseTerm": "text", "ExtendedWarrantyFlag": "text", "Salesman_3_Number": "text", "Salesman_3_Name": "text", "Salesman_4_Number": "text", "Salesman_4_Name": "text", "Salesman_5_Number": "text", "Salesman_5_Name": "text", "Salesman_6_Number": "text", "Salesman_6_Name": "text", "APRRate2": "text", "APRRate3": "text", "APRRate4": "text", "Term2": "text", "SecurityDeposit2": "text", "DownPayment2": "text", "TotalOfPayments2": "text", "BasePayment": "text", "JournalSaleAmount": "text", "IndividualBusinessFlag": "text", "InventoryDate": "text", "StatusDate": "text", "ListPrice": "text", "NetTradeAmount": "text", "TrimLevel": "text", "SubTrimLevel": "text", "BodyDescription": "text", "BodyDoorCount": "text", "TransmissionDesc": "text", "EngineDesc": "text", "TypeCode": "text", "SLCT2": "text", "DealDateOffset": "text", "AccountingDate": "text", "CoBuyerCustNum": "text", "CoBuyerCell": "text", "CoBuyerEmail": "text", "CoBuyerSalutation": "text", "CoBuyerPhoneBlock": "text", "CoBuyerMailBlock": "text", "CoBuyerEmailBlock": "text", "RealBookDate": "text", "CoBuyerMiddleName": "text", "CoBuyerCountry": "text", "CoBuyerAddress2": "text", "CoBuyerOptOut": "text", "CoBuyerOccupation": "text", "CoBuyerEmployer": "text", "Country": "text", "Occupation": "text", "Employer": "text", "Salesman2Commission": "text", "BankAddress": "text", "BankCity": "text", "BankState": "text", "BankZip": "text", "LeaseEstimatedMiles": "text", "AFTReserve": "text", "CreditLifePrem": "text", "CreditLifeRes": "text", "AHRes": "text", "Language": "text", "BuyRate": "text", "DMVAmount": "text", "Weight": "text", "StateDMVTotFee": "text", "ROSNumber": "text", "Incentives": "text", "CASS_STD_LINE1": "text", "CASS_STD_LINE2": "text", "CASS_STD_CITY": "text", "CASS_STD_STATE": "text", "CASS_STD_ZIP": "text", "CASS_STD_ZIP4": "text", "CASS_STD_DPBC": "text", "CASS_STD_CHKDGT": "text", "CASS_STD_CART": "text", "CASS_STD_LOT": "text", "CASS_STD_LOTORD": "text", "CASS_STD_URB": "text", "CASS_STD_FIPS": "text", "CASS_STD_EWS": "text", "CASS_STD_LACS": "text", "CASS_STD_ZIPMOV": "text", "CASS_STD_Z4LOM": "text", "CASS_STD_NDIAPT": "text", "CASS_STD_NDIRR": "text", "CASS_STD_LACSRT": "text", "CASS_STD_ERROR_CD": "text", "NCOA_AC_ID": "text"}
	sv := map[string]string{"FileType": "text", "ACDealerID": "text", "ClientDealerID": "text", "DMSType": "text", "RONumber": "text", "OpenDate": "text", "CustomerNumber": "text", "CustomerName": "text", "CustomerFirstName": "text", "CustomerLastName": "text", "CustomerAddress": "text", "CustomerCity": "text", "CustomerState": "text", "CustomerZip": "text", "CustomerHomePhone": "text", "CustomerWorkPhone": "text", "CustomerCellPhone": "text", "CustomerEmail": "text", "CustomerBirthdate": "text", "VehicleMileage": "text", "VehicleYear": "text", "VehicleMake": "text", "VehicleModel": "text", "VehicleVIN": "text", "ServiceAdvisorNumber": "text", "ServiceAdvisorName": "text", "TechnicianName": "text", "TechnicianNumber": "text", "DeliveryDate": "text", "OperationCode": "text", "OperationDescription": "text", "ROAmount": "text", "WarrantyName": "text", "WarrantyExpirationDate": "text", "WarrantyExpirationMiles": "text", "SalesmanNumber": "text", "SalesmanName": "text", "ClosedDate": "text", "LaborTypes": "text", "WarrantyLaborAmount": "text", "WarrantyPartJobSale": "text", "WarrantyMiscAmount": "text", "WarrantyRepairOrderTotal": "text", "InternalLaborSale": "text", "InternalPartsSale": "text", "InternalMiscAmount": "text", "InternalRepairOrderTotal": "text", "CustomerPayLaborAmount": "text", "CustomerPayPartsSale": "text", "CustomerPayMiscSale": "text", "CustomerPayRepairOrderTotal": "text", "LaborCostDollar": "text", "PartsCostDollar": "text", "MiscCostDollar": "text", "MiscDollar": "text", "LaborDollar": "text", "PartsDollar": "text", "VehicleColor": "text", "CustomerPayPartsCost": "text", "CustomerPayLaborCost": "text", "CustomerPayGOGCost": "text", "CustomerPaySubletCost": "text", "CustomerPayMiscCost": "text", "WarrantyPartsCost": "text", "WarrantyLaborCost": "text", "WarrantyGOGCost": "text", "WarrantySubletCost": "text", "WarrantyMiscCost": "text", "InternalPartsCost": "text", "InternalLaborCost": "text", "InternalGOGCost": "text", "InternalSubletCost": "text", "InternalMiscCost": "text", "TotalTax": "text", "TotalLaborHours": "text", "TotalBillHours": "text", "ServiceComment": "text", "LaborComplaint": "text", "LaborBillingRate": "text", "LaborTechnicianRate": "text", "AppointmentFlag": "text", "MailBlock": "text", "EmailBlock": "text", "PhoneBlock": "text", "ROInvoiceDate": "text", "ROCustomerPayPostDate": "text", "ROStatus": "text", "MechanicNumber": "text", "ROMileage": "text", "DeliveryMileage": "text", "StockNumber": "text", "RecommendedService": "text", "Recommendations": "text", "CustomerSuffix": "text", "CustomerSalutation": "text", "CustomerAddress2": "text", "CustomerMiddleName": "text", "GlobalOptOut": "text", "PromiseDate": "text", "PromiseTime": "text", "ROLogon": "text", "LaborTypes2": "text", "LanguagePreference": "text", "MiscCode": "text", "MiscCodeAmount": "text", "PartNumber": "text", "PartDescription": "text", "PartQuantity": "text", "MiscCodeDescription": "text", "MakePrefix": "text", "Department": "text", "ROTotalCost": "text", "PipedComplaint": "text", "PipedComment": "text", "MileageOut": "text", "IndividualBusinessFlag": "text", "CustGOGSale": "text", "LaborHours": "text", "BillingHours": "text", "TagNo": "text", "StockType": "text", "ROOpenTime": "text", "CustSUBSale": "text", "WarrGOGSale": "text", "WarrSUBSale": "text", "IntlGOGSale": "text", "IntlSUBSale": "text", "TotalGOGCost": "text", "TotalGOGSale": "text", "TotalSUBCost": "text", "TotalSUBSale": "text", "Model#": "text", "Transmission": "text", "EngineConfig": "text", "TrimLevel": "text", "PaymentMethod": "text", "PickupDate": "text", "CustGender": "text", "JobStatus": "text", "CASS_STD_LINE1": "text", "CASS_STD_LINE2": "text", "CASS_STD_CITY": "text", "CASS_STD_STATE": "text", "CASS_STD_ZIP": "text", "CASS_STD_ZIP4": "text", "CASS_STD_DPBC": "text", "CASS_STD_CHKDGT": "text", "CASS_STD_CART": "text", "CASS_STD_LOT": "text", "CASS_STD_LOTORD": "text", "CASS_STD_URB": "text", "CASS_STD_FIPS": "text", "CASS_STD_EWS": "text", "CASS_STD_LACS": "text", "CASS_STD_ZIPMOV": "text", "CASS_STD_Z4LOM": "text", "CASS_STD_NDIAPT": "text", "CASS_STD_NDIRR": "text", "CASS_STD_LACSRT": "text", "CASS_STD_ERROR_CD": "text", "NCOA_AC_ID": "text"}
	sv_appt := map[string]string{"FileType": "text", "ACDealerID": "text", "ClientDealerID": "text", "DMSType": "text", "AppointmentNumber": "text", "RONumber": "text", "CustomerName": "text", "CustomerHomePhone": "text", "CustomerEmailAddress": "text", "AppointmentDate": "text", "AppointmentTime": "text", "VehicleYear": "text", "VehicleMake": "text", "VehicleModel": "text", "VehicleVIN": "text", "ServiceAdvisorNumber": "text", "OperationCode": "text", "ComplaintStatement": "text", "Comments": "text", "CustomerFirstName": "text", "CustomerLastName": "text", "CustomerAddress": "text", "CustomerCity": "text", "CustomerState": "text", "CustomerZip": "text", "CustomerCellPhone": "text", "CustomerNumber": "text", "CustomerWorkPhone": "text", "Department": "text", "CASS_STD_LINE1": "text", "CASS_STD_LINE2": "text", "CASS_STD_CITY": "text", "CASS_STD_STATE": "text", "CASS_STD_ZIP": "text", "CASS_STD_ZIP4": "text", "CASS_STD_DPBC": "text", "CASS_STD_CHKDGT": "text", "CASS_STD_CART": "text", "CASS_STD_LOT": "text", "CASS_STD_LOTORD": "text", "CASS_STD_URB": "text", "CASS_STD_FIPS": "text", "CASS_STD_EWS": "text", "CASS_STD_LACS": "text", "CASS_STD_ZIPMOV": "text", "CASS_STD_Z4LOM": "text", "CASS_STD_NDIAPT": "text", "CASS_STD_NDIRR": "text", "CASS_STD_LACSRT": "text", "CASS_STD_ERROR_CD": "text", "NCOA_AC_ID": "text"}
	l := len(rec)

	for i := 1; i < l; i++ {
		var mapType map[string]string
		var queryStr string
		if len(rec[i]) == 0 {
			continue
		}
		switch rec[i][0] {
		case "SV":
			mapType = sv
		case "SV_APPT":
			mapType = sv_appt
		case "SL":
			mapType = sl
		}
		queryStr = "INSERT INTO " + rec[i][0] + " VALUES ("
		for j, c := range rec[i] {
			var quote string
			switch mapType[rec[0][j]] {
			case "text":
				quote = "'"
			case "date":
				quote = "'"
			case "int":
				quote = ""
			}
			addString := string(c)
			switch {
			case strings.Contains(addString, "'"):
				addString = strings.Replace(addString, "'", "''", -1)
			case addString == "\"\"":
				addString = "''"
			}
			addString = quote + addString + quote
			queryStr += addString + ","
		}
		queryStr = queryStr[:len(queryStr)-1] + ")"
		//PrintDeb("queryStr=", queryStr)
		//rows, err := db.Query(queryStr) // try Exec
		_, err := db.Exec(queryStr) // !!!!!!!!!!!!!
		if err != nil {
			CLog.PrintLog(false, "Error execute INSERT INTO ", rec[i][0], ". ", err)
		}
		//defer rows.Close()
	}
	return
}

func getFTPFiles(chFileNames chan string) {
	var vFTPConn FTPConn
	if err := os.Chdir("_temp_"); err != nil {
		if err := os.Mkdir("_temp_", os.ModeDir); err != nil {
			CLog.PrintLog(true, "Error create directory: '_temp_'. ", err)
			os.Exit(1)
		}
		if err := os.Chdir("_temp_"); err != nil {
			CLog.PrintLog(true, "Can't change directory: '_temp_'. ", err)
			os.Exit(1)
		}
	}

	// get FTP config
	ftpHostPort, _ := Conf.GetValue("FTP", "FTP Host & Port")
	if ftpHostPort == "" {
		CLog.PrintLog(true, "Section [FTP], parameter 'FTP Host & Port' is empty.")
		os.Exit(1)
	}
	ftpUser, _ := Conf.GetValue("FTP", "FTP User Name")
	ftpPass, _ := Conf.GetValue("FTP", "FTP Password")
	ftpTimeout, _ := Conf.Int("FTP", "TimeOut")
	if ftpTimeout <= 0 || ftpTimeout > 600 {
		ftpTimeout = 90
	}
	// connect and login to ftp server
	if err := vFTPConn.FTPLogin(ftpHostPort, ftpUser, ftpPass, ftpTimeout); err != nil {
		CLog.PrintLog(true, "Error login to ftp-server. ", ftpHostPort, ". ", err)
		os.Exit(1)
	}

	dirname, _ := Conf.GetValue("FTP", "FTP Dir")
	if err := vFTPConn.ChangeDir(dirname); err != nil {
		CLog.PrintLog(true, "Can't change directory: "+ftpHostPort+"/"+dirname+" . ", err)
		os.Exit(1)
	}

	list, err := vFTPConn.List("*.TXT")
	if err != nil {
		CLog.PrintLog(true, "Can't get list of files from: "+ftpHostPort+"/"+dirname+" . ", err)
		os.Exit(1)
	}

	for _, fi := range list {
		var good bool
		good, err = vFTPConn.FTPFile(fi)
		if good {
			wgFTP.Add(1)
			chFileNames <- fi.Name
		}
	}
	wgFTP.Wait()
	vFTPConn.Quit()
	close(chFileNames)
	os.Chdir("..")
	if err := os.Remove("_temp_"); err != nil {
		CLog.PrintLog("Can't remove the temporary directory '_temp_'.")
	}
	return
}
