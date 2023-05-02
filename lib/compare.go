package lib 


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
				Comment: v.Comment,
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
						ColumnType: ca.ColumnType,
						NullAllowed: ca.NullAllowed,
						Comment: ca.Comment,
						Status: 1,
					}
				} else if b[k].Columns[kca].Definfo != ca.Definfo {
					// Modify Column
					columnRaws[kca] = ColumnRawData{
						Definfo: ca.Definfo,
						ColumnType: ca.ColumnType,
						NullAllowed: ca.NullAllowed,
						Comment: ca.Comment,
						Status: 2,
					}
				}
			}

			if b[k].TableDef != v.TableDef {
				// Modify Table
				Compares[k] = TableRaw{
					TableDef: v.TableDef,
					Columns: columnRaws,
					Comment: v.Comment,
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

			for kcb,kcv := range v.Columns {
				if a[k].Columns[kcb].Definfo == "" {
					// Drop Column
					columnRaws[kcb] = ColumnRawData{
						Definfo: "",
						ColumnType: kcv.ColumnType,
						NullAllowed: kcv.NullAllowed,
						Comment: kcv.Comment,
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
		case 1:
			// ADD
			_, err = o.Object.Exec(addTableQuery,k,v.TableDef,v.Comment)
			if err != nil {
				return err
			}
			for vck, vc := range v.Columns {
				_, err = o.Object.Exec(addColumnQuery,k,vck,vc.Definfo,vc.ColumnType,vc.NullAllowed,vc.Comment)
				if err != nil {
					return err
				}
			}
		case 2:
			// Modify
			_, err = o.Object.Exec(modifyTableQuery,v.TableDef,v.Comment,k)
			if err != nil {
				return err
			}
		case 9:
			// Drop
			_, err = o.Object.Exec(droTableQuery,k)
			if err != nil {
				return err
			}

			_, err = o.Object.Exec(dropTableColumnQuery,k)
			if err != nil {
				return err
			}
		}
		for ck, cv := range v.Columns {
			switch cv.Status {
			case 1:
				// ADD
				_, err = o.Object.Exec(addColumnQuery,k,ck,cv.Definfo,cv.ColumnType,cv.NullAllowed,cv.Comment)
				if err != nil {
					return err
				}
			case 2:
				// Modify
				_, err = o.Object.Exec(modifyColumnQuery,cv.Definfo,cv.ColumnType,cv.NullAllowed,cv.Comment,k,ck)
				if err != nil {
					return err
				}
			case 9:
				// Drop
				_, err = o.Object.Exec(dropColumnQuery,k,ck)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
