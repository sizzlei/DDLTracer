package lib

import (
	"fmt"
	"errors"
	// "github.com/sizzlei/confloader"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"database/sql"
)

type DDLTracerConfigure struct {
	Global 			GlobalConfigure	`yaml:"Global"`
	Targets			[]Target		`yaml:"Targets"`
}

type GlobalConfigure struct {
	User 		string 				`yaml:"User"`
	Pass 		string 				`yaml:"Pass"`
	Webhook		string 				`yaml:"WebhookUrl"`
	DBPath		string 				`yaml:"DBPath"`
}

type Target struct {
	Alias			string 			`yaml:"Alias"`
	Endpoint		string			`yaml:"Endpoint"`
	Port			int64			`yaml:"Port"`
	DB				[]string		`yaml:"DB"`
	MyObj 			*sql.DB
	LiteObj			*sql.DB
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

func (c DDLTracerConfigure) TargetLoad() ([]string) {
	var aliasList []string
	for _,v := range c.Targets {
		aliasList = append(aliasList,v.Alias)
	}
	return aliasList
}