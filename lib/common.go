package lib

import (
	"fmt"
	"errors"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"database/sql"
)

type DDLTracerConfigure struct {
	Global 			GlobalConfigure	`yaml:"Global"`				// Global Option
	Targets			[]Target		`yaml:"Targets"` 			// Target Configure
}

type GlobalConfigure struct {
	User 		string 				`yaml:"User,omitempty"`
	Pass 		string 				`yaml:"Pass,omitempty"`				
	Webhook		string 				`yaml:"WebhookUrl"`			// Notification Slack Channel
	DBPath		string 				`yaml:"DBPath"`				// SQLite Save Path
	CompareIv 	int64 				`yaml:"Compare_interval"`	// Compare routine exec interval(Seconds)
}

type Target struct {
	Alias			string 			`yaml:"Alias"`				// Cluster or instance Identifier
	Endpoint		string			`yaml:"Endpoint"`			// Connect Endpoint
	Port			int64			`yaml:"Port"`				// DB Port (default : 3306)
	DB				[]string		`yaml:"DB"`					// DB Array list
}

type DBObject struct {
	Object		*sql.DB
}


func GetOpt(msg string) (*string, error){
	var x string 
	fmt.Printf("%s : ",msg)
	_, _ = fmt.Scanf("%s",&x)
	if len(x) == 0 {
		return nil, errors.New(fmt.Sprintf("Not %s Args.",msg))
	}

	return &x, nil
}

// String convert point
func PointerStr(s string) *string {
	str := s 
	return &str
}

func ConfigureLoad(p string) (DDLTracerConfigure, error) {
	var c DDLTracerConfigure 
	yamlFile, _ := ioutil.ReadFile(p)
	err := yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		return c, err
	}

	return c, nil
}

// Configure Target convert array
func (c DDLTracerConfigure) TargetLoad() ([]string) {
	var aliasList []string
	for _,v := range c.Targets {
		aliasList = append(aliasList,v.Alias)
	}
	return aliasList
}