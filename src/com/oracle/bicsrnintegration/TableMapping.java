package com.oracle.bicsrnintegration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import com.oracle.bics.apis.table.objects.DateTableColumn;
import com.oracle.bics.apis.table.objects.NumberTableColumn;
import com.oracle.bics.apis.table.objects.Table;
import com.oracle.bics.apis.table.objects.VarcharTableColumn;

public class TableMapping {

	private String rightNowTableName;
	private String tableName;
	private ArrayList<String> rightNowColumns;
	private Table t;
	int lineNum = 1;
	
	private TableMapping() {
		rightNowColumns = new ArrayList<String>();
		t = new Table();
	}
	
	public static TableMapping read(File file) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
			
			TableMapping tm = new TableMapping();
			tm.setHeader(br.readLine());
			
			String attr;
			while ((attr = br.readLine()) != null) {
				tm.addAttribute(attr);
			}
			
			return tm;
		} catch (FileNotFoundException e) {
			Log.log(Module.TABLEREADER, "File with table properties was not found! Aborting...");
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			Log.log(Module.TABLEREADER, "There was a problem reading table! Aborting...");
			e.printStackTrace();
			System.exit(1);
		} finally {
			if (br != null)
				try {
					br.close();
				} catch (IOException e) {
					Log.log(Module.TABLEREADER, "Couldn't close table properties reader resource! Aborting...");
					e.printStackTrace();
					System.exit(1);
				}
		}
		
		return null;
	}
	
	public void setHeader(String header) {
		String[] headerMapping = header.split("\\,");
		rightNowTableName = headerMapping[0];
		tableName = headerMapping[1];
	}
	
	public void addAttribute(String attr) {
		lineNum++;
				
		String[] record = attr.split("\\,");
		
		// right now name
		rightNowColumns.add(record[0].toUpperCase());
		
		if (record[1].length() >= 30) {
			Log.log(Module.TABLEREADER, "Error in line '" + lineNum + "': alias '" + record[1].toUpperCase() + "' must have 30 characters or less. Aborting...");
			System.exit(1);
		}
				
		// bics table
		if (record[2].equals("VARCHAR")) {
			t.add(new VarcharTableColumn(record[1].toUpperCase(), Integer.valueOf(record[3]), isNullable(record[4])));
		} else if (record[2].equals("DATE") || record[2].equals("TIMESTAMP")) {
			t.add(new DateTableColumn(record[1].toUpperCase(), isNullable(record[3])));
		} else if (record[2].equals("NUMBER")) {
			t.add(new NumberTableColumn(record[1].toUpperCase(), Integer.valueOf(record[3]), Integer.valueOf(record[4]), isNullable(record[5])));
		} else {
			Log.log(Module.TABLEREADER, "Error in line '" + lineNum + "': '" + record[0].toUpperCase() + "' is not a valid data type! Aborting...");
			System.exit(1);
		}		
	}
	
	private boolean isNullable(String attr) {
		if (attr.equals("NULLABLE")) {
			return true;
		} else if (attr.equals("IS NOT NULL")){
			return false;
		} else {
			Log.log(Module.TABLEREADER, "Error in line '" + lineNum + "': '" + attr + "' is not recognized. Expecting 'NULLABLE' or 'IS NOT NULL'. Aborting...");
			System.exit(1);
			return true;
		}
	}

	public String getRightNowTableName() {
		return rightNowTableName;
	}

	public String getTableName() {
		return tableName;
	}

	public ArrayList<String> getRightNowColumns() {
		return rightNowColumns;
	}

	public Table getTable() {
		return t;
	}
	
}
