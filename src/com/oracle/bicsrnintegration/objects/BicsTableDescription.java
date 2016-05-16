package com.oracle.bicsrnintegration.objects;

import com.oracle.bics.BicsConnection;
import com.oracle.bics.apis.tabledata.TableDataApi;
import com.oracle.bics.apis.tabledata.objects.TableData;

public class BicsTableDescription {
	
	private TableDataApi tableDataApi;
	private String tableName;
	private TableData tableData;
	
	public BicsTableDescription(BicsConnection bicsConnection, String tableName, TableData tableData) {
		tableDataApi = new TableDataApi(bicsConnection);
		this.tableName = tableName;
		this.tableData = tableData;
	}

	public TableDataApi getTableDataApi() {
		return tableDataApi;
	}

	public String getTableName() {
		return tableName;
	}

	public TableData getTableData() {
		return tableData;
	}
		
}
