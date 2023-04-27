package lib 

import (
	"fmt"
)


func CompareTable(a map[string]TableRaw, b map[string]TableRaw) map[string]TableRaw {
	//a - DB , b - sqlite 
	var Compares map[string]TableRaw
	Compares = make(map[string]TableRaw)
	// Add or Modify
	for k, v := range a {
		if b[k].TableDef == "" {
			// Add
			Compares[k] = TableRaw{
				TableDef: v.TableDef,
				Columns: v.Columns,
				Status: 1,
			}
		} else {
			var columnRaws map[string]ColumnRawData
			columnRaws = make(map[string]ColumnRawData)

			for kca, ca := range v.Columns {
				if b[k].Columns[kca].Definfo == "" {
					// Add Column
					columnRaws[kca] = ColumnRawData{
						Definfo: ca.Definfo,
						Status: 1,
					}
				} else if b[k].Columns[kca].Definfo != ca.Definfo {
					// Modify Column
					columnRaws[kca] = ColumnRawData{
						Definfo: ca.Definfo,
						Status: 2,
					}
				}
			}

			if b[k].TableDef != v.TableDef {
				// Modify Table
				Compares[k] = TableRaw{
					TableDef: v.TableDef,
					Columns: columnRaws,
					Status: 2,
				}
			} else {
				if len(columnRaws) > 0 {
					// None or Table None Columns
					Compares[k] = TableRaw{
						TableDef: v.TableDef,
						Columns: columnRaws,
						Status: 0,
					}
				}
			}
		} 
	}

	// Drop
	for k, v := range b {
		if a[k].TableDef == "" {
			Compares[k] = TableRaw{
				TableDef: v.TableDef,
				Columns: v.Columns,
				Status: 9,
			}
		} else {
			var columnRaws map[string]ColumnRawData
			columnRaws = make(map[string]ColumnRawData)

			if len(Compares[k].Columns) > 0 {
				columnRaws = Compares[k].Columns
			}

			for kcb,_ := range v.Columns {
				if a[k].Columns[kcb].Definfo == "" {
					// Drop Column
					columnRaws[kcb] = ColumnRawData{
						Definfo: "",
						Status: 9,
					}
				}
			}

			if len(columnRaws) > 0 {
				// None Table
				Compares[k] = TableRaw{
					TableDef: v.TableDef,
					Columns: columnRaws,
					Status: 0,
				}
			}
		
		}

	}

	return Compares
}

func (t Target) DeployCompare(s string, c map[string]TableRaw) error {
	addTableQuery := `
		INSERT INTO %s_table_definitions(table_name,def_info)
		values (?,?)
	`

	addColumnQuery := `
		INSERT INTO %s_column_definitions(table_name,column_name,def_info)
		values (?,?,?)
	`


	droTableQuery := `
		DELETE FROM %s_table_definitions WHERE table_name = ?
	`

	dropTableColumnQuery := `
		DELETE FROM %s_column_definitions WHERE table_name = ?
	`
	dropColumnQuery := `
		DELETE FROM %s_column_definitions WHERE table_name = ? and column_name = ?
	`

	modifyTableQuery := `
		UPDATE %s_table_definitions 
		SET def_info = ?
		WHERE table_name = ?
	`

	modifyColumnQuery := `
		UPDATE %s_column_definitions 
		SET def_info = ?
		WHERE table_name = ? and column_name = ?
	`


	var err error
	for k, v := range c {
		switch v.Status {
		case 1:
			// ADD
			_, err = t.LiteObj.Exec(fmt.Sprintf(addTableQuery,s),k,v.TableDef)
			if err != nil {
				return err
			}
			for vck, vc := range v.Columns {
				_, err = t.LiteObj.Exec(fmt.Sprintf(addColumnQuery,s),k,vck,vc.Definfo)
				if err != nil {
					return err
				}
			}
		case 2:
			// Modify
			_, err = t.LiteObj.Exec(fmt.Sprintf(modifyTableQuery,s),v.TableDef,k)
			if err != nil {
				return err
			}
		case 9:
			// Drop
			_, err = t.LiteObj.Exec(fmt.Sprintf(droTableQuery,s),k)
			if err != nil {
				return err
			}

			_, err = t.LiteObj.Exec(fmt.Sprintf(dropTableColumnQuery,s),k)
			if err != nil {
				return err
			}
		}
		for ck, cv := range v.Columns {
			switch cv.Status {
			case 1:
				// ADD
				_, err = t.LiteObj.Exec(fmt.Sprintf(addColumnQuery,s),k,ck,cv.Definfo)
				if err != nil {
					return err
				}
			case 2:
				// Modify
				_, err = t.LiteObj.Exec(fmt.Sprintf(modifyColumnQuery,s),cv.Definfo,k,ck)
				if err != nil {
					return err
				}
			case 9:
				// Drop
				_, err = t.LiteObj.Exec(fmt.Sprintf(dropColumnQuery,s),k,ck)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
