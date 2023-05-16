package main 

import (
	log "github.com/sirupsen/logrus"
	"flag"
	"fmt"
	"DDLTracer/lib"
	"github.com/sizzlei/confloader"
	"os"
	"strings"
	"time"
	"sync"
)

const (
	appName	= "DDL Tracer for MySQL by DBA"
)

var conf lib.DDLTracerConfigure
var Noti = make(chan lib.NotiChannel)
var histClean bool
func main() {
	// Flag Parsing
	var mode,confPath,authDiv,paramKey,region string 
	flag.StringVar(&mode,"mode","","DDLTracer Mode(INIT / START)")
	flag.StringVar(&authDiv,"auth","CONF","DDLTracer authentication method(CONF / PARAM), Param is AWS Parameter Store.")
	flag.StringVar(&confPath,"conf","./config.yml","DDLTracer Configure")
	flag.StringVar(&region,"region","ap-northeast-2","DDLTracer authentication Parameter store Region")
	flag.StringVar(&paramKey,"key","","DDLTracer authentication Parameter store key")
	flag.BoolVar(&histClean,"history-clean",false,"Definition History Clean")
	flag.Parse()

	log.Infof("%s",appName)

	// Mode
	// INIT : Sqlite Schema & Data Reset
	// START : Schema Scan
	if mode == "" {
		log.Errorf("invalid mode(INIT / START)")
		os.Exit(1)
	}

	var err error
	// Configure Load 
	// Need change Parameter Store 
	conf, err = lib.ConfigureLoad(confPath)
	if err != nil {
		log.Errorf("Failed to Configure Load")
		log.Errorf("[main.ConfigureLoad] %s",err)
		os.Exit(1)
	}

	switch strings.ToUpper(authDiv) {
	case "CONF":
		if conf.Global.User == "" || conf.Global.Pass == "" {
			log.Errorf("Invalid Global User and Global Password Please Check configure file.")	
			os.Exit(1)
		}
	case "PARAM":
		if paramKey != "" {
			// AWS Parameter Store
			var paramData confloader.Param
			paramData, err = confloader.AWSParamLoader(region, paramKey)
			if err != nil {
				log.Errorf("[main.AWSParamLoader] %s",err)
				os.Exit(1)
			}

			// Load Auth conf for Param Data
			paramAuth := paramData.Keyload("DDLTracer")

			// Set Global User & Pass
			conf.Global.User = paramAuth["User"]
			conf.Global.Pass = paramAuth["Pass"]
			log.Infof("Parameter load complete for %s:%s",region,paramKey)

		} else {
			log.Errorf("Invalid Parameter Store Key. Please Check -h")	
			os.Exit(1)
		}
	default:
		log.Errorf("Invalid authentication. Please Check -h")
		os.Exit(1)
	}
	

	// Configure Check
	if conf.Global.CompareIv < 60 && strings.ToUpper(mode) == "START" {
		log.Warningf("Compare interval is less than 60s.")
		log.Warningf("Compare interval Set 60s")
		conf.Global.CompareIv = 30
	}

	switch strings.ToUpper(mode) {
	case "INIT":
		log.Infof("Tager List:")
		// Target load for configure
		l :=  conf.TargetLoad()
		for _, v := range l{
			fmt.Println("-",v)
		}

		// Choice target cluster
		initTarget, err := lib.GetOpt("Init Target(Single or All(Enter))")
		if err != nil {
			log.Warningf("%s",err)
		}

		// Declare DB Target
		var dbs []lib.Target
		if initTarget != nil {
			for _, v := range conf.Targets {
				if v.Alias == *initTarget {
					dbs = append(dbs,v)
				}
			}
		} else {
			for _, v := range conf.Targets {
				dbs = append(dbs,v)
			}
		}
		
		// Initialize Clusters
		var wg sync.WaitGroup
		wg.Add(len(dbs))
		for _, v := range dbs {
			go InitServer(&wg,v)
		}

		wg.Wait()
	
	case "START":
		var wg sync.WaitGroup
		wg.Add(len(conf.Targets))

		// Notification rouine for channel
		go CompareFollowUp(Noti)

		for _, t := range conf.Targets {
			// schema compare routine
			go CompareServer(&wg,t)
		}

		wg.Wait()
	default:
		log.Errorf("Invalid Mode. Please Check -h")
	}
}


func CompareServer(wg *sync.WaitGroup,z lib.Target) {
	// Main routine to run schema-specific routines
	defer wg.Done()
	for {
		var swg sync.WaitGroup
		swg.Add(len(z.DB))
		for _, s := range z.DB {
			// sub routines by schema
			go CompareDB(&swg, z, s)
		}

		swg.Wait()
		time.Sleep(time.Duration(conf.Global.CompareIv)*time.Second)
	}
}

func CompareDB(swg *sync.WaitGroup,t lib.Target, s string) {
	defer swg.Done()

	var liteObj,myObj lib.DBObject
	var err error

	// Source DB Connections(SQLite)
	liteObj.Object, _, err = lib.OpenSQLite(conf.Global.DBPath,t.Alias,s)
	if err != nil {
		log.Errorf("[CompareDB.OpenSQLite] %s",err)
		return
	}
	defer liteObj.Object.Close()

	// Target DB Connections (MySQL)
	myObj.Object, err = lib.CreateDBObject(t,conf.Global.User,conf.Global.Pass)
	if err != nil {
		log.Errorf("[CompareDB.CreateDBObject] %s",err)
		return
	}
	defer myObj.Object.Close()

	// Target db get definitions
	aRawData, err := myObj.GetDefinitions(s)
	if err != nil {
		log.Errorf("[CompareDB.GetDefinitions] %s",err)
		return
	}

	// Source db get definitions(Save Data)
	bRawData, err := liteObj.GetLiteDefinitions()
	if err != nil {
		log.Errorf("[CompareDB.GetLiteDefinitions] %s",err)
		return
	}

	// Compare Schema
	Compares := lib.CompareTable(aRawData,bRawData)

	// Compare result deploy
	err = liteObj.DeployCompare(Compares)
	if err != nil {
		log.Errorf("[CompareDB.DeployCompare] %s",err)
		return
	}

	if len(Compares) > 0 {
		// Send Compare result Channel
		Noti <- lib.NotiChannel{
			Alias: t.Alias,
			Schema: s,
			Compares: Compares,
		}

		// History Write
		err = liteObj.WriteHistory(Compares)
		if err != nil {
			log.Errorf("[CompareDB.WriteHistory] %s",err)
			return
		}
	}
}

func InitServer(wg *sync.WaitGroup,z lib.Target) error {
	// Main routine to run schema-specific routines
	defer wg.Done()
	var swg sync.WaitGroup
	swg.Add(len(z.DB))

	for _, s := range z.DB {
		// sub routines by schema
		go InitDB(&swg, z, s)
	}

	swg.Wait()

	return nil
}

func InitDB(swg *sync.WaitGroup,t lib.Target, s string) {
	defer swg.Done()
	var liteObj,myObj lib.DBObject
	var err error
	var fileExists bool
	log.Infof("%s:%s Initialize Start",t.Alias, s)
	liteObj.Object, fileExists, err = lib.OpenSQLite(conf.Global.DBPath,t.Alias,s)
	if err != nil {
		log.Errorf("[InitDB.OpenSQLite] %s %s",t.Alias,err)
		return
	}

	myObj.Object, err = lib.CreateDBObject(t,conf.Global.User,conf.Global.Pass)
	if err != nil {
		log.Errorf("[InitDB.CreateDBObject] %s %s",t.Alias,err)
		return
	}
	defer myObj.Object.Close()

	// History Table Check
	if fileExists == false && histClean == false {
		// First Init 
		histClean = true
	}

	// Init Storage Table
	err = liteObj.InitSchema(s,histClean)
	if err != nil {
		log.Errorf("[InitDB.InitSchema] %s %s",t.Alias,err)
		return
	}
	
	// Get Definition
	rawData, err := myObj.GetDefinitions(s)
	if err != nil {
		log.Errorf("[InitDB.GetDefinitions] %s %s",t.Alias,err)
		return
	}

	// Write Definition
	err = liteObj.WriteDefinitions(rawData)
	if err != nil {
		log.Errorf("[InitDB.WriteDefinitions] %s %s",t.Alias,err)
		return
	}

	log.Infof("%s:%s Initialize Complete",t.Alias, s)
}

func CompareFollowUp(ch <-chan lib.NotiChannel) {
	for i := range ch {
		now := time.Now().Format("2006-01-02 15:04:05")
		fmt.Println(now, i.Alias, i.Schema, i.Compares)
		// Notification
		err := lib.TraceNotification(appName, i, conf.Global.Webhook,conf.Global.AddTableView)
		if err != nil {
			log.Errorf("[CompareFollowUp.TraceNotification] %s",err)
			return
		}
	}
}