package edu.buffalo.cse562;

import java.io.Serializable;

public class MyColumn {
	
	/**
	 * 
	 */
	
	private String value;
	private String alias;
	
	private boolean isDouble;
	
	private boolean isInteger;
	
	private String columnName;
	
	private String tableName;
	public MyColumn(String name, String value, boolean isDub, boolean isInt, String tableName)
	{
		columnName = name;
		this.value = value;
		isDouble = isDub;
		isInteger = isInt;
		this.tableName = tableName;
	}
	public MyColumn(String name, String alias,  String value, boolean isDub, boolean isInt, String tableName)
	{
		columnName = name;
		this.value = value;
		isDouble = isDub;
		isInteger = isInt;
		this.tableName = tableName;
		this.alias = alias;
	}
	public String getValue()
	{
		return value;
	}

	public double getDoubleValue(){
		return Double.parseDouble(value);
	}
	
	public int getIntegerValue(){
		return Integer.parseInt(value);
	}
	
	public boolean isDouble(){
		return isDouble;
	}
	
	public boolean isInteger(){
		return isInteger;
	}
	
	public String getName(){
		return columnName;
	}
	
	public String getTableName() {
		return tableName;
	}
	public String getAlias()
	{
		return alias;
	}
	public void setAlias(String alias)
	{
		this.alias = alias;
	}
	
	public void setValue(String val){
		this.value = val;
	}
	
}
