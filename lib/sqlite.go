package lib

import (
	"database/sql"
	_ "modernc.org/sqlite"
	"fmt"
	"time"
)

func OpenSQLite(p string,a string, s string) (*sql.DB, error) {
	sqlObj, err := sql.Open("sqlite",fmt.Sprintf("%s/%s_%s.db",p, s, a))
	if err != nil {
		return nil, err
	}
	
	return sqlObj,nil
}

func (o DBObject) InitSchema(s string,c bool) error {
	DropColumnsQuery := `DROP TABLE IF EXISTS column_definitions;`
	DropTableQuery := `DROP TABLE IF EXISTS table_definitions;`
	DropHistoryQuery := `DROP TABLE IF EXISTS definition_history;`

	CreateColumnsQuery := `
		CREATE TABLE column_definitions (
			table_name text not null,
			column_name text not null,
			def_info text not null,
			column_type text not null,
			nullallowed text not null,
			comment text not null,
			PRIMARY KEY (table_name,column_name)
		);
	`

	CreateTableQuery := `
		CREATE TABLE table_definitions (
			table_name text not null,
			def_info text not null,
			comment text not null,
			PRIMARY KEY (table_name)
		);
	`

	CreateHistoryQuery := `
		CREATE TABLE definition_history (
			table_name text not null,
			column_name text null default null,
			status text not null,
			def_info text null default null,
			created_dt text not null
		);
	`

	CreateHistoryIndex := `
			CREATE INDEX idx_tablename_columnname ON definition_history (table_name,column_name)
	`

	// Drop
	_, err := o.Object.Exec(DropColumnsQuery)
	if err != nil {
		return err
	}

	_, err = o.Object.Exec(DropTableQuery)
	if err != nil {
		return err
	}

	// Create
	_, err = o.Object.Exec(CreateColumnsQuery)
	if err != nil {
		return err
	}

	_, err = o.Object.Exec(CreateTableQuery)
	if err != nil {
		return err
	}

	// History Cleaner
	if c == true {
		_, err = o.Object.Exec(DropHistoryQuery)
		if err != nil {
			return err
		}

		_, err = o.Object.Exec(CreateHistoryQuery)
		if err != nil {
			return err
		}

		_, err = o.Object.Exec(CreateHistoryIndex)
		if err != nil {
			return err
		}
	}
	

	return nil
}

func (o DBObject) WriteDefinitions(r map[string]TableRaw) error {
	addTableQuery := `
		INSERT INTO table_definitions(table_name,def_info,comment)
		VALUES(?,?,?)
	`

	addColumnQuery := `
		INSERT INTO column_definitions(table_name,column_name,def_info,column_type,nullallowed,comment)
		VALUES (?,?,?,?,?,?)
	`

	for k, v := range r {
		// Table
		_, err := o.Object.Exec(addTableQuery,k, v.TableDef,v.Comment)
		if err != nil {
			return err
		}

		for c, d := range v.Columns {
			_, err := o.Object.Exec(addColumnQuery,k,c,d.Definfo,d.ColumnType,d.NullAllowed,d.Comment)
			if err != nil {
				return err
			}
		}
	} 

	return nil
}

func (o DBObject) GetLiteDefinitions() (map[string]TableRaw, error) {
	getTableQuery := `
		SELECT 
			table_name,
			def_info,
			comment
		FROM table_definitions
	`

	getColumnQuery := `
		select 
			column_name,
			def_info,
			column_type,
			nullallowed,
			comment
		from column_definitions
		where table_name = ?
	`
	
	var Raws map[string]TableRaw
	Raws = make(map[string]TableRaw)

	data, err := o.Object.Query(getTableQuery)
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

		columnsData, err := o.Object.Query(getColumnQuery,table)
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

		Raws[table] = TableRaw{
			TableDef: definfo,
			Columns: columnRaws,
			Comment: tableComment,
		}
	}

	return Raws, nil
}

func (o DBObject) WriteHistory(c map[string]TableRaw) error {
	now := time.Now().Format("2006-01-02 15:04:05")

	Queries := `
		INSERT INTO definition_history(table_name,column_name,status,def_info,created_dt)
		VALUES (?,?,?,?,?);
	`

	for k, v := range c {
		switch v.Status {
		// Create Table
		case 1:
			_, err := o.Object.Exec(Queries,k,nil,"add",v.TableDef,now)
			if err != nil {
				return err
			}
			// add column for new table
			for sck, scv := range v.Columns {
				_, err := o.Object.Exec(Queries,k,sck,"add",scv.Definfo,now)
				if err != nil {
					return err
				}
			}
		// Modify table
		case 2:
			_, err := o.Object.Exec(Queries,k,nil,"modify",v.TableDef,now)
			if err != nil {
				return err
			}
		// Drop table
		case 9:
			_, err := o.Object.Exec(Queries,k,nil,"drop",nil,now)
			if err != nil {
				return err
			}
		}
		// for column
		for ck, cv := range v.Columns {
			switch cv.Status {
			// add column
			case 1:
				_, err := o.Object.Exec(Queries,k,ck,"add",cv.Definfo,now)
				if err != nil {
					return err
				}
			// modify column
			case 2:
				_, err := o.Object.Exec(Queries,k,ck,"modify",cv.Definfo,now)
				if err != nil {
					return err
				}
			// drop column
			case 9:
				_, err := o.Object.Exec(Queries,k,ck,"drop",nil,now)
				if err != nil {
					return err
				}
			}
		}

	}
	return nil
}

func (o DBObject) DeployCompare(c map[string]TableRaw) error {
	addTableQuery := `
		INSERT INTO table_definitions(table_name,def_info,comment)
		values (?,?,?)
	`

	addColumnQuery := `
		INSERT INTO column_definitions(table_name,column_name,def_info,column_type,nullallowed,comment)
		values (?,?,?,?,?,?)
	`


	droTableQuery := `
		DELETE FROM table_definitions WHERE table_name = ?
	`

	dropTableColumnQuery := `
		DELETE FROM column_definitions WHERE table_name = ?
	`
	dropColumnQuery := `
		DELETE FROM column_definitions WHERE table_name = ? and column_name = ?
	`

	modifyTableQuery := `
		UPDATE table_definitions 
		SET def_info = ?,
			comment = ?
		WHERE table_name = ?
	`

	modifyColumnQuery := `
		UPDATE column_definitions 
		SET def_info = ?,
			column_type = ?,
			nullallowed = ?,
			comment = ?
		WHERE table_name = ? and column_name = ?
	`


	var err error
	for k, v := range c {
		switch v.Status {
		// Create table
		case 1:
			_, err = o.Object.Exec(addTableQuery,k,v.TableDef,v.Comment)
			if err != nil {
				return err
			}
			// add column for new table
			for vck, vc := range v.Columns {
				_, err = o.Object.Exec(addColumnQuery,k,vck,vc.Definfo,vc.ColumnType,vc.NullAllowed,vc.Comment)
				if err != nil {
					return err
				}
			}
		// modify table
		case 2:
			_, err = o.Object.Exec(modifyTableQuery,v.TableDef,v.Comment,k)
			if err != nil {
				return err
			}
		// drop table
		case 9:
			_, err = o.Object.Exec(droTableQuery,k)
			if err != nil {
				return err
			}
			// drop column for origin table
			_, err = o.Object.Exec(dropTableColumnQuery,k)
			if err != nil {
				return err
			}
		}
		// for column
		for ck, cv := range v.Columns {
			switch cv.Status {
			// add column
			case 1:
				_, err = o.Object.Exec(addColumnQuery,k,ck,cv.Definfo,cv.ColumnType,cv.NullAllowed,cv.Comment)
				if err != nil {
					return err
				}
			// modify column
			case 2:
				_, err = o.Object.Exec(modifyColumnQuery,cv.Definfo,cv.ColumnType,cv.NullAllowed,cv.Comment,k,ck)
				if err != nil {
					return err
				}
			// drop column
			case 9:
				_, err = o.Object.Exec(dropColumnQuery,k,ck)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}