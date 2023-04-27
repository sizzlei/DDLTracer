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
		for _, v := range dbs {
			// Create Database Object
			v.LiteObj, err = v.OpenSQLite(conf.Global.DBPath)
			if err != nil {
				log.Errorf("%s",err)
				os.Exit(1)
			}
			defer v.LiteObj.Close()

			v.MyObj, err = conf.Global.CreateDBObject(v)
			if err != nil {
				log.Errorf("%s",err)
				os.Exit(1)
			}
			defer v.MyObj.Close()
			log.Infof("%s Initalize Start",v.Alias)
			
			for _, s := range v.DB {
				// Init Storage Table
				err := v.InitSchema(s)
				if err != nil {
					log.Errorf("%s",err)
					os.Exit(1)
				}
				log.Infof("%s Initialize",s)

				// Get Definition
				rawData, err := v.GetDefinitions(s)
				if err != nil {
					log.Errorf("%s",err)
					os.Exit(1)
				}

				// Write Definition
				err = v.WriteDefinitions(s, rawData)
				if err != nil {
					log.Errorf("%s",err)
					os.Exit(1)
				}

			}

			log.Infof("%s initialize Complete",v.Alias)
		}
	
	case "START":
		var wg sync.WaitGroup
		wg.Add(len(conf.Targets))

		go Notificator(Noti)

		for _, t := range conf.Targets {
			err := CompareDB(&wg,t)
			if err != nil {
				log.Errorf("%s",err)
			}
		}

		wg.Wait()
	}
}

func CompareDB(wg *sync.WaitGroup,z lib.Target) error {
	defer wg.Done()
	var err error
	// Create Database Object
	z.LiteObj, err = z.OpenSQLite(conf.Global.DBPath)
	if err != nil {
		return err
	}
	defer z.LiteObj.Close()

	z.MyObj, err = conf.Global.CreateDBObject(z)
	if err != nil {
		return err
	}
	defer z.MyObj.Close()

	for {
		for _, s := range z.DB {
			aRawData, err := z.GetDefinitions(s)
			if err != nil {
				return err
			}

			bRawData, err := z.GetLiteDefinitions(s)
			if err != nil {
				return err
			}

			// Compare Table
			Compares := lib.CompareTable(aRawData,bRawData)

			err = z.DeployCompare(s,Compares)
			if err != nil {
				return err
			}

			if len(Compares) > 0 {
				Noti <- lib.NotiChannel{
					Schema: s,
					Compares: Compares,
				}
			}
		}

		time.Sleep(10*time.Second)
	}

	return nil
}

func Notificator(ch <-chan lib.NotiChannel) {
	for i := range ch {
		fmt.Println(i)
	}
}