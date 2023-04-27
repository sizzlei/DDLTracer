package lib

import (
	"database/sql"
	_ "modernc.org/sqlite"
	"fmt"
)


func (t Target) OpenSQLite(p string) (*sql.DB, error) {
	sqlObj, err := sql.Open("sqlite",fmt.Sprintf("%s/%s_%s.db",p, t.Alias,t.Endpoint))
	if err != nil {
		return nil, err
	}
	

	return sqlObj,nil
}

func (t Target) InitSchema(s string) error {
	DropColumnsQuery := `DROP TABLE IF EXISTS %s_column_definitions;`
	DropTableQuery := `DROP TABLE IF EXISTS %s_table_definitions;`

	CreateColumnsQuery := `
		CREATE TABLE %s_column_definitions (
			table_name text not null,
			column_name text not null,
			def_info text not null,
			PRIMARY KEY (table_name,column_name)
		);
	`

	CreateTableQuery := `
		CREATE TABLE %s_table_definitions (
			table_name text not null,
			def_info text not null,
			PRIMARY KEY (table_name)
		);
	`

	// Drop
	_, err := t.LiteObj.Exec(fmt.Sprintf(DropColumnsQuery,s))
	if err != nil {
		return err
	}

	_, err = t.LiteObj.Exec(fmt.Sprintf(DropTableQuery,s))
	if err != nil {
		return err
	}

	// Create
	_, err = t.LiteObj.Exec(fmt.Sprintf(CreateColumnsQuery,s))
	if err != nil {
		return err
	}

	_, err = t.LiteObj.Exec(fmt.Sprintf(CreateTableQuery,s))
	if err != nil {
		return err
	}

	return nil
}

func (t Target) WriteDefinitions(s string, r map[string]TableRaw) error {
	addTableQuery := `
		INSERT INTO %s_table_definitions(table_name,def_info)
		VALUES(?,?)
	`

	addColumnQuery := `
		INSERT INTO %s_column_definitions(table_name,column_name,def_info)
		VALUES (?,?,?)
	`

	for k, v := range r {
		// Table
		_, err := t.LiteObj.Exec(fmt.Sprintf(addTableQuery,s),k, v.TableDef)
		if err != nil {
			return err
		}

		for c, d := range v.Columns {
			_, err := t.LiteObj.Exec(fmt.Sprintf(addColumnQuery,s),k,c,d.Definfo)
			if err != nil {
				return err
			}
		}
	} 

	return nil
}

func (t Target) GetLiteDefinitions(s string) (map[string]TableRaw, error) {
	getTableQuery := `
		SELECT 
			table_name,
			def_info
		FROM %s_table_definitions
	`

	getColumnQuery := `
		select 
			column_name,
			def_info
		from %s_column_definitions
		where table_name = ?
	`
	
	var Raws map[string]TableRaw
	Raws = make(map[string]TableRaw)

	data, err := t.LiteObj.Query(fmt.Sprintf(getTableQuery,s))
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

		columnsData, err := t.LiteObj.Query(fmt.Sprintf(getColumnQuery,s),table)
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

