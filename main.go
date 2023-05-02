package main 

import (
	log "github.com/sirupsen/logrus"
	"flag"
	"fmt"
	"DDLTracer/lib"
	"os"
	"strings"
	"time"
	"sync"
)

const (
	appName	= "DDLTracer"
	desc 	= "MySQL DDL Tracer"
	version	= "v0.1.0"
)

var conf lib.DDLTracerConfigure
var Noti = make(chan lib.NotiChannel)

func main() {
	var mode,confPath string 
	flag.StringVar(&mode,"mode","","DDLTracer Mode(INIT / START)")
	flag.StringVar(&confPath,"conf","","DDLTracer Configure")
	flag.Parse()

	log.Infof("%s %s",appName,version)
	log.Infof("%s",desc)

	if mode == "" {
		log.Errorf("invalid mode(INIT / START)")
		os.Exit(1)
	}

	var err error
	// Configure Load
	conf, err = lib.ConfigureLoad(confPath)
	if err != nil {
		log.Errorf("Failed to Configure Load")
		log.Errorf("%s",err)
		os.Exit(1)
	}

	switch strings.ToUpper(mode) {
	case "INIT":
		log.Infof("Tager List:")
		l :=  conf.TargetLoad()
		for _, v := range l{
			fmt.Println("-",v)
		}

		// Config Path
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
		
		// Main Init
		var wg sync.WaitGroup
		wg.Add(len(dbs))
		for _, v := range dbs {
			go InitServer(&wg,v)
		}

		wg.Wait()
	
	case "START":
		var wg sync.WaitGroup
		wg.Add(len(conf.Targets))

		go CompareFollowUp(Noti)

		for _, t := range conf.Targets {
			go CompareServer(&wg,t)
		}

		wg.Wait()
	}
}

func CompareServer(wg *sync.WaitGroup,z lib.Target) {
	defer wg.Done()
	for {
		var swg sync.WaitGroup
		swg.Add(len(z.DB))
		for _, s := range z.DB {
			go CompareDB(&swg, z, s)
		}

		swg.Wait()
		time.Sleep(10*time.Second)
	}
}

func CompareDB(swg *sync.WaitGroup,t lib.Target, s string) {
	defer swg.Done()

	var liteObj,myObj lib.DBObject
	var err error
	liteObj.Object, err = lib.OpenSQLite(conf.Global.DBPath,t.Alias,s)
	if err != nil {
		log.Errorf("OpenSQLite : %s",err)
		return
	}

	myObj.Object, err = lib.CreateDBObject(t,conf.Global.User,conf.Global.Pass)
	if err != nil {
		log.Errorf("CreateDBObject : %s",err)
		return
	}
	defer myObj.Object.Close()

	aRawData, err := myObj.GetDefinitions(s)
	if err != nil {
		log.Errorf("GetDefinitions : %s",err)
		return
	}

	bRawData, err := liteObj.GetLiteDefinitions()
	if err != nil {
		log.Errorf("GetLiteDefinitions : %s",err)
		return
	}

	// Compare Table
	Compares := lib.CompareTable(aRawData,bRawData)

	// Compare Deploy
	err = liteObj.DeployCompare(Compares)
	if err != nil {
		log.Errorf("DeployCompare : %s",err)
		return
	}

	if len(Compares) > 0 {
		// Send Notification Channel
		Noti <- lib.NotiChannel{
			Schema: s,
			Compares: Compares,
		}

		// History Write
		err = liteObj.WriteHistory(Compares)
		if err != nil {
			log.Errorf("WriteHistory : %s",err)
			return
		}
		
	}
}

func InitServer(wg *sync.WaitGroup,z lib.Target) error {
	defer wg.Done()
	var swg sync.WaitGroup
	swg.Add(len(z.DB))

	log.Infof("%s Initalize.",z.Alias)

	for _, s := range z.DB {
		go InitDB(&swg, z, s)
	}

	swg.Wait()

	return nil
}

func InitDB(swg *sync.WaitGroup,t lib.Target, s string) {
	defer swg.Done()
	var liteObj,myObj lib.DBObject
	var err error
	log.Infof("%s:%s Initialize Start",t.Alias, s)
	liteObj.Object, err = lib.OpenSQLite(conf.Global.DBPath,t.Alias,s)
	if err != nil {
		log.Errorf("OpenSQLite : %s",err)
		return
	}

	myObj.Object, err = lib.CreateDBObject(t,conf.Global.User,conf.Global.Pass)
	if err != nil {
		log.Errorf("CreateDBObject : %s",err)
		return
	}
	defer myObj.Object.Close()

	// Init Storage Table
	err = liteObj.InitSchema(s)
	if err != nil {
		log.Errorf("InitSchema : %s",err)
		return
	}
	
	// Get Definition
	rawData, err := myObj.GetDefinitions(s)
	if err != nil {
		log.Errorf("GetDefinitions : %s",err)
		return
	}

	// Write Definition
	err = liteObj.WriteDefinitions(rawData)
	if err != nil {
		log.Errorf("WriteDefinitions : %s",err)
		return
	}

	log.Infof("%s:%s Initialize Complete",t.Alias, s)
}

func CompareFollowUp(ch <-chan lib.NotiChannel) {
	for i := range ch {
		err := lib.TraceNotification(i,conf.Global.Webhook)
		if err != nil {
			log.Errorf("CompareFollowUp : %s",err)
			return
		}
	}
}