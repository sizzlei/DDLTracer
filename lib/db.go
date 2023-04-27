package lib

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
)

type TableRaw struct{
	TableDef 	string
	Columns 	map[string]ColumnRawData
	Status		int64 // Status - 1:Add / 2:Modify / 9:Drop
}

type ColumnRawData struct {
	Definfo		string
	Status		int64 // Status - 1:Add / 2:Modify / 9:Drop
}

func (g GlobalConfigure) CreateDBObject(t Target) (*sql.DB,error) {
	DSN := "%s:%s@tcp(%s:%d)/information_schema"
	// Create DB Object
	dbObj, err := sql.Open("mysql",fmt.Sprintf(DSN,g.User,g.Pass,t.Endpoint,t.Port))
	if err != nil {
		return nil,err
	}

	var result int
	err = dbObj.QueryRow("select 1").Scan(&result)
	if err != nil {
		return nil,err
	}

	return dbObj,nil
} 

func (t Target) GetDefinitions(s string) (map[string]TableRaw, error) {
	getTableQuery := `
		SELECT 
			table_name,
			concat_ws("|",
			IFNULL(TABLE_TYPE,"NULL"),
			IFNULL(ROW_FORMAT,"NULL"),
			IFNULL(TABLE_COLLATION,"NULL"),
			TABLE_COMMENT)
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
			)
		from information_schema.columns
		where table_schema = ?
			and table_name = ?
	`
	
	var Raws map[string]TableRaw
	Raws = make(map[string]TableRaw)

	data, err := t.MyObj.Query(getTableQuery,s)
	if err != nil {
		return Raws, err
	}
	defer data.Close()

	for data.Next() {
		var table,definfo string
		err := data.Scan(
			&table,
			&definfo,
		)
		if err != nil {
			return Raws, err
		}

		columnsData, err := t.MyObj.Query(getColumnQuery,s,table)
		if err != nil {
			return Raws, err
		}
		defer columnsData.Close()

		var columnRaws map[string]ColumnRawData
		columnRaws = make(map[string]ColumnRawData)

		for columnsData.Next() {
			var columnName,defInfo string
			err := columnsData.Scan(
				&columnName,
				&defInfo,
			)
			if err != nil {
				return Raws, err
			}

			columnRaws[columnName] = ColumnRawData{
				Definfo: defInfo,
			}
		}

		Raws[table] = TableRaw{
			TableDef: definfo,
			Columns: columnRaws,
		}
	}

	return Raws, nil
}
