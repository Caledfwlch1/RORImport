// ftp
package main

import (
	"bufio"
	"github.com/jlaffaye/ftp"
	"io"
	"os"
	"time"
	"database/sql"
	"errors"
	//"strings"
	"fmt"
)

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
	//writeBytes, err := io.Copy(writer, reader)

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

func getFTPFiles(dealerID sharedMap, chFileNames chan string, db *sql.DB) {
	var vFTPConn FTPConn
	//var queryStr string
	var id int
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
		os.Exit(1)
	}

	dirname, _ := Conf.GetValue("FTP", "FTP Dir")
	if err := vFTPConn.ChangeDir(dirname); err != nil {
		CLog.PrintLog(true, "Can't change directory: "+ftpHostPort+"/"+dirname+" . ", err)
		os.Exit(1)
	}

	list, err := vFTPConn.List("DFC*.TXT")
	if err != nil {
		CLog.PrintLog(true, "Can't get list of files from: "+ftpHostPort+"/"+dirname+" . ", err)
		os.Exit(1)
	}
	
	//dealerId := make(map[string]int)
	
	for _, fi := range list {
		id, err = findDealerID(dealerID, fi.Name[:7], db)
		if err != nil {
			CLog.PrintLog(true, "Error search dealer_id in dealers. ", err)
			continue
		}
		if err := insertS3fileToDB(fi.Name, id, fi.Size, db); err != nil {
			CLog.PrintLog(true, "Error insert to the s3files. ", err)
			continue
		}
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
	return
}

func insertS3fileToDB(name string, dealer_id int, size uint64, db *sql.DB) (err error) {
	var status string
	
	queryStr:= "SELECT status FROM s3files WHERE name='" + name + "';"
	err = db.QueryRow(queryStr).Scan(&status)
	if err == sql.ErrNoRows {
		queryStr = "INSERT INTO s3files (name, size, status, dealer_id) VALUES ('" + name + "', " + fmt.Sprint(size) + ", 'registered', " + fmt.Sprint(dealer_id) + ");"
		_, err = db.Exec(queryStr)
		return err
	}
	if status == "moved" || status == "registered" || status == "hold" {
		return errors.New("The file " + name + " is registered and status '" + status + "'.")
	}
	return err
}