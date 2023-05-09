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

func TraceNotification(app string, n NotiChannel, url string, colView bool) error {
	data := `
		{
			"Color" : "#fd7e14",
			"blocks": [
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": ":database: *%s*"
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
						"text": "*Action:* %s *Table:* %s "
					}
				}
			`
			action := ConvertStatus(v.Status)

			sections = append(sections,fmt.Sprintf(section,fmt.Sprintf("`%s`",action),fmt.Sprintf("`%s(%s)`",k,v.Comment)))
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
				sections = append(sections,fmt.Sprintf(tableSection,fmt.Sprintf("`%s (%s)`",k,v.Comment)))
			}

			if colView == false && v.Status == 1 {
				continue
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
				
				cAction := ConvertStatus(cv.Status)

				var nullString string
				if cv.NullAllowed == "YES" {
					nullString = "NULL"
				} else {
					nullString = "NOT NULL"
				}

				diffColumn = append(diffColumn,fmt.Sprintf(columnFormat,fmt.Sprintf("`%s` _*%s*_ `%s %s comment '%s'`",cAction, ck, cv.ColumnType, nullString, cv.Comment)))
				
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

	err = slack.SendWebhookAttchment(url,fmt.Sprintf("*%s*",app),att)
	if err != nil {
		return err
	}

	return nil

}

func ConvertStatus(status string) string {
	switch status {
	case 1:
		return "ADD"
	case 2: 
		return "MODIFY"
	case 9:
		return "DROP"
	case 0:
		return "NONE"
	}

	return "invalid_status"
}