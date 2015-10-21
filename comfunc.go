package main

import (
	"fmt"
	"github.com/Unknwon/goconfig"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	LSTR = "\r\n"
)

type CommonLog struct {
	log.Logger
}

var CLog CommonLog
var IniFile string

type CongSet struct {
	goconfig.ConfigFile
}

var Conf CongSet

func InitFunc() *os.File {
	// The directory run the application makes the working directory of the application.
	os.Chdir(os.Args[0][:strings.LastIndex(os.Args[0], string(os.PathSeparator))])

	// read config file
	IniFile = "rorimport.ini"
	if err := Conf.ReadINI(); err != nil {
		PrintDeb(err)
		os.Exit(1)
		return nil
	}
	// open/create log-file
	filog := LoadLog()

	return filog
}

// create/open log-file
func LoadLog() (logFileWr *os.File) {
	nameLogFile, _ := Conf.GetValue("Logging", "Log_File")
	// check is exist a log-file
	if _, err := os.Stat(nameLogFile); err != nil {
		logFileWr, err = os.Create(nameLogFile)
		if err != nil {
			PrintDeb(err)
			return nil
		}
	}
	logFileWr, err := os.OpenFile(nameLogFile, os.O_WRONLY, 0666)
	if err != nil {
		PrintDeb(err)
		return nil
	}
	// go to end file
	logFileWr.Seek(0, os.SEEK_END)
	c := log.New(logFileWr, "", 0)
	CLog.Logger = *c
	name, _ := procName(true, 3)
	CLog.PrintLog(false, " ################   Run "+name+".   ################ ")
	return logFileWr
}

// the function return the name of working function
func procName(shortName bool, level int) (name string, line int) {
	pc, _, line, _ := runtime.Caller(level)
	name = runtime.FuncForPC(pc).Name()
	if shortName {
		name = name[strings.Index(name, ".")+1:]
	}
	return name, line
}

// the function for print log-record into file and/or to screen (console = true)
func (v *CommonLog) PrintLog(console bool, s ...interface{}) {
	var str string
	str, _ = os.Hostname()
	str = time.Now().String() + " ; " + str
	str += " ; " + fmt.Sprint(s...) // + LSTR
	v.Println(str)
	if console {
		fmt.Println(str)
	}
	if err := v.Output(2, ""); err != nil {
		PrintDeb(err)
	}
	return
}

// read the config file
func (c *CongSet) ReadINI() (err error) {
	if _, err := os.Stat(IniFile); err != nil {
		if err := CreateDefaultConfig(); err != nil {
			return err
		}
	}
	conf, err := goconfig.LoadConfigFile(IniFile)
	c.ConfigFile = *conf
	return nil
}

// create the default config
func CreateDefaultConfig() error {
	confFile, err := os.Create(IniFile)
	defer confFile.Close()
	if err != nil {
		fmt.Println(err)
		return err
	}
	defConf := "# Rorinport ini-file." +
		LSTR + "[Default]" +
		LSTR + "Host = localhost" +
		LSTR + "DataBase = test" +
		LSTR + "User Name = admin" +
		LSTR + "Password = " +
		LSTR + "Quantity Proc = 3" +
		LSTR +
		LSTR + "[FTP]" +
		LSTR + "FTP Host & Port = localhost:21" +
		LSTR + "FTP User Name = test" +
		LSTR + "FTP Password = test" +
		LSTR + "TimeOut = 90" +
		LSTR + "FTP Dir = tmp" +
		LSTR +
		LSTR + "[Logging]" +
		LSTR + "Log_File = rorinport.log" +
		LSTR + LSTR

	_, err = fmt.Fprintln(confFile, defConf)

	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

// the function for debugging,
// it print function name, number of string and specified of variables
func PrintDeb(s ...interface{}) {
	name, line := procName(false, 2)
	fmt.Print("=> ", name, " ", line, ": ")
	fmt.Println(s...)
	return
}
