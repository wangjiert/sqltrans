package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cihub/seelog"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/robfig/config"
)

var (
	Engine   *xorm.Engine
	Config   *config.Config
	Logger   seelog.LoggerInterface
	MYDDir   string
	TransDir string
	DataDir  string
	OutDir   string
	Dbname   string
	User     string
	Password string
	Host     string
)

func init() {
	var err error
	Config, err = config.ReadDefault("conf/my.conf")
	if err != nil {
		panic(err)
	}

	driver, _ := Config.String("database", "db.driver")
	Dbname, _ = Config.String("database", "db.dbname")
	User, _ = Config.String("database", "db.user")
	Password, _ = Config.String("database", "db.password")
	Host, _ = Config.String("database", "db.host")
	params := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true", User, Password, Host, Dbname) + "&loc=Asia%2FChongqing"

	Engine, err = xorm.NewEngine(driver, params)
	if err != nil {
		panic(err)
	}

	Logger, err = seelog.LoggerFromConfigAsFile("conf/log.xml")
	if err != nil {
		panic(err)
	}
	MYDDir, _ = Config.String("myd", "MYDDir")
	TransDir, _ = Config.String("myd", "transDir")
	OutDir, _ = Config.String("myd", "out")
	DataDir, _ = Config.String("myd", "data")
}

func getHead(table string) string {
	result, err := Engine.QueryString("select column_name from information_schema.columns where table_name=?", table)
	if err != nil {
		Logger.Error(err)
		return ""
	}
	returnStr := ""
	if len(result) > 0 {
		for key, _ := range result {
			returnStr += result[key]["column_name"] + "|"
		}
		returnStr = strings.TrimRight(returnStr, "|")
	}
	return returnStr
}

var fileExtend = []string{".MYD", ".frm", ".MYI"}

func parseFile(tableName, dirName string) {
	srcFilePrefix := filepath.Join(dirName, tableName)
	destFilePrefix := filepath.Join(MYDDir, tableName)
	for _, ext := range fileExtend {
		srcFile, err := os.Open(srcFilePrefix + ext)
		if err != nil {
			Logger.Error(err)
			return
		}
		defer srcFile.Close()

		destFile, err := os.OpenFile(destFilePrefix+ext, os.O_RDWR|os.O_TRUNC|os.O_CREATE, os.ModePerm)
		if err != nil {
			Logger.Error(err)
			return
		}
		defer destFile.Close()
		err = os.Chmod(destFilePrefix+ext, os.ModePerm)
		if err != nil {
			Logger.Error(err)
			return
		}

		_, err = io.Copy(destFile, srcFile)
		if err != nil {
			Logger.Error(err)
			return
		}
	}

	head := getHead(tableName)
	if head == "" {
		Logger.Errorf("can't read data head")
		return
	}

	buffer := bytes.NewBuffer(nil)
	dump := exec.Command("sh", "-c", fmt.Sprintf("mysqldump -u %s -h %s -p%s %s %s -T %s --fields-terminated-by='|'", User, strings.Split(Host, ":")[0], Password, Dbname, tableName, TransDir))
	dump.Stderr = buffer
	if err := dump.Run(); err != nil {
		Logger.Error(string(buffer.Bytes()))
		return
	}

	outputFile := filepath.Join(TransDir, tableName+".txt")
	if err := os.Rename(outputFile, filepath.Join(OutDir, tableName+".txt")); err != nil {
		os.Remove(outputFile)
		Logger.Error(err)
		return
	}

	outputFile = filepath.Join(OutDir, tableName+".txt")
	combine := exec.Command("sh", "-c", fmt.Sprintf("sed -i '1i\\%s' %s", head, outputFile))
	buffer.Reset()
	combine.Stderr = buffer
	if err := combine.Run(); err != nil {
		os.Remove(outputFile)
		Logger.Error(string(buffer.Bytes()))
		return
	}
}

func main() {
	defer Logger.Flush()
	parseFile("import_data_x", DataDir)
}
