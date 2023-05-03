package lib

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
)

type TableRaw struct{
	TableDef 	string
	Columns 	map[string]ColumnRawData
	Comment 	string
	Status		int64 // Status - 1:Add / 2:Modify / 9:Drop / 0:None
}

type ColumnRawData struct {
	Definfo		string
	ColumnType 	string 
	NullAllowed	string 
	Comment 	string
	Status		int64 // Status - 1:Add / 2:Modify / 9:Drop / 0:None
}

func CreateDBObject(t Target, u string, p string) (*sql.DB,error) {
	DSN := "%s:%s@tcp(%s:%d)/information_schema"
	// Create DB Object
	dbObj, err := sql.Open("mysql",fmt.Sprintf(DSN,u,p,t.Endpoint,t.Port))
	if err != nil {
		return nil,err
	}

	// Connection Check
	err = dbObj.Ping()
	if err != nil {
		return nil, err
	}

	return dbObj,nil
} 

func (o DBObject) GetDefinitions(s string) (map[string]TableRaw, error) {
	getTableQuery := `
		SELECT 
			table_name,
			concat_ws("|",
			IFNULL(TABLE_TYPE,"NULL"),
			IFNULL(ROW_FORMAT,"NULL"),
			IFNULL(TABLE_COLLATION,"NULL"),
			TABLE_COMMENT),
			TABLE_COMMENT
		FROM information_schema.tables where table_schema = ?
	`

	getColumnQuery := `
		select 
			column_name,
			concat_ws("|",
				is_nullable,
				IFNULL(column_default,"NULL"),
				IFNULL(character_set_name,"NULL"),
				IFNULL(collation_name,"NULL"),
				column_type,
				IF(extra="","NULL",extra),
				IF(generation_expression="","NULL",extra),
				IF(column_comment="","NULL",column_comment)
			),
			column_type,
			is_nullable,
			IF(column_comment="","NULL",column_comment)
		from information_schema.columns
		where table_schema = ?
			and table_name = ?
	`
	
	var Raws map[string]TableRaw
	Raws = make(map[string]TableRaw)

	data, err := o.Object.Query(getTableQuery,s)
	if err != nil {
		return Raws, err
	}
	defer data.Close()

	for data.Next() {
		var table,definfo,tableComment string
		err := data.Scan(
			&table,
			&definfo,
			&tableComment,
		)
		if err != nil {
			return Raws, err
		}

		// Get Columns
		columnsData, err := o.Object.Query(getColumnQuery,s,table)
		if err != nil {
			return Raws, err
		}
		defer columnsData.Close()

		var columnRaws map[string]ColumnRawData
		columnRaws = make(map[string]ColumnRawData)

		for columnsData.Next() {
			var columnName,defInfo,columnType,nullallowed,columnComment string
			err := columnsData.Scan(
				&columnName,
				&defInfo,
				&columnType,
				&nullallowed,
				&columnComment,
			)
			if err != nil {
				return Raws, err
			}

			columnRaws[columnName] = ColumnRawData{
				Definfo: defInfo,
				ColumnType: columnType,
				NullAllowed: nullallowed,
				Comment: columnComment,
			}
		}

		// Create definitions set
		Raws[table] = TableRaw{
			TableDef: definfo,
			Columns: columnRaws,
			Comment: tableComment,
		}
	}

	return Raws, nil
}
