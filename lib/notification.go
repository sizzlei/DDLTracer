package lib

import (
	slack "github.com/sizzlei/slack-notificator"
	"fmt"
	"strings"
)

type NotiChannel struct {
	Schema 		string 
	Compares 	map[string]TableRaw
}

func TraceNotification(n NotiChannel, url string) error {
	data := `
		{
			"Color" : "#fd7e14",
			"blocks": [
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "Schema: *%s*"
					}
				},
				%s
			]
		}
	`

	var sections []string
	for k, v := range n.Compares {
		
		if v.Status > 0 {
			section := `
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "*Table:* %s *Action:* %s *Changed:* %s"
					}
				}
			`
			var action string 
			switch v.Status {
			case 1:
				action = "ADDED"
			case 2: 
				action = "MODIFIED"
			case 9:
				action = "DROPPED"
			}

			sections = append(sections,fmt.Sprintf(section,fmt.Sprintf("`%s`",k),fmt.Sprintf("`%s`",action),fmt.Sprintf("`%s`",v.TableDef)))
		}

		if len(v.Columns) > 0 && v.Status != 9 {
			if v.Status == 0 {
				tableSection := `
					{
						"type": "section",
						"text": {
							"type": "mrkdwn",
							"text": "*Table:* %s"
						}
					}
				`
				sections = append(sections,fmt.Sprintf(tableSection,fmt.Sprintf("`%s`",k)))
			}

			section := `
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "*Information:* \n%s"
					}
				}
			`
			
			var diffColumn []string
			for ck, cv := range v.Columns {
				columnFormat := `%s \n`
				
				var cAction string 
				switch cv.Status {
				case 1:
					cAction = "ADD"
				case 2: 
					cAction = "MODIFY"
				case 9:
					cAction = "DROP"
				case 0:
					cAction = "NONE"
				}

				var nullString string
				if cv.NullAllowed == "YES" {
					nullString = "NULL"
				} else {
					nullString = "NOT NULL"
				}

				diffColumn = append(diffColumn,fmt.Sprintf(columnFormat,fmt.Sprintf("`%s` _%s_ `%s %s comment '%s'`",cAction, ck, cv.ColumnType, nullString, cv.Comment)))
				
			}
			diffMsg := strings.Join(diffColumn,"")

			sections = append(sections,fmt.Sprintf(section,diffMsg))
		}
	}
	sectionsStr := strings.Join(sections,",")
	sendMsg := fmt.Sprintf(data,n.Schema,sectionsStr)

	att, err := slack.CreateAttachement(sendMsg)
	if err != nil {
		return err
	}

	err = slack.SendWebhookAttchment(url,"*DDL Tracer for MySQL by DBA*",att)
	if err != nil {
		return err
	}

	return nil

}