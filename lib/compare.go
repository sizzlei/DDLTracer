package lib 

func CompareTable(a map[string]TableRaw, b map[string]TableRaw) map[string]TableRaw {
	//a - DB , b - sqlite 
	var Compares map[string]TableRaw
	Compares = make(map[string]TableRaw)
	// Add or Modify
	for k, v := range a {
		if b[k].TableDef == "" {
			// Table Addd
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
						Comment: v.Comment,
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
				Comment: v.Comment,
				Status: 9,
			}
		} else {
			var columnRaws map[string]ColumnRawData
			columnRaws = make(map[string]ColumnRawData)

			// If there is existing data, it is reflected including that data
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
					Comment: v.Comment,
					Status: 0,
				}
			}
		
		}

	}

	return Compares
}
