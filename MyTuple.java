package edu.buffalo.cse562;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;




public class MyTuple implements Comparable<MyTuple>{
	/**
	 * 
	 */
	

	List<MyColumn> tuple;
	
	//int numberOfColumns;
	
	private MyTable table; 
	
	static ArrayList<Integer> orderByColumns;
	
	static ArrayList<Integer> groupByColumns;
	
/*	public MyTuple (int noOfColumns, MyTable table)
	{
		//numberOfColumns = noOfColumns;
		tuple = new ArrayList<MyColumn>();
		this.table = table;
	}*/
	public MyTuple (MyTable table)
	{
		//numberOfColumns = noOfColumns;
		tuple = new ArrayList<MyColumn>();
		this.table = table;
	}
	public List<MyColumn> getTupleList()
	{
		return tuple;
	}
	public MyColumn getColumn(int pos)
	{
		return tuple.get(pos);
	}
	public void AddColumn(MyColumn col)
	{
		tuple.add(col);
	}
	public void AddColumns(List<MyColumn> t)
	{
		for(MyColumn c : t)
		{
			AddColumn(c);
		}
	}
/*	public int getSize()
	{
		return numberOfColumns;
	}*/
	public static MyTable concatSchema(MyTuple tl, MyTuple tr)
	{
		MyTable t = new MyTable(new ArrayList<MyTuple>(),null);
		t.setName(tl.getTable().getTablename()+"_"+tr.getTable().getTablename());
		
		int position = t.columnOrder.size();	
	    for(MyColumn c : tl.getTupleList())
	    {
	    	//newTuple.AddColumn(c);
	    	t.columnOrder.put(c.getName().toUpperCase(), position);
         	t.columnOrder.put(tl.getTable().tableName.toUpperCase() +"_"+
	    			c.getName().toUpperCase(), position);
	    	position++;
	    }
		for(MyColumn c : tr.getTupleList())
		{
			//newTuple.AddColumn(c);
			t.columnOrder.put(c.getName().toUpperCase(), position);
			t.columnOrder.put(tr.getTable().tableName.toUpperCase() +"_"+
	    	    c.getName().toUpperCase(), position);
	    	position++;
		}
		return t;
	}

	public static MyTuple ConcatTuple(MyTuple tl, MyTuple tr)
	{
		MyTuple newTuple = null;
		//MyTable t = new MyTable(new ArrayList<MyTuple>(2));
		//t.columnOrder = tl.getTable().columnOrder;
		//t.columnOrder.putAll(tr.getTable().columnOrder);
		if(tl != null && tr != null){ 
			//t.setName(tl.getTable().getTablename()+"_"+tr.getTable().getTablename());
			newTuple = new MyTuple(null);
			//int position = t.columnOrder.size();	
		    for(MyColumn c : tl.getTupleList())
		    {
		    	newTuple.AddColumn(c);
//		    	newTuple.getTable().columnOrder.put(c.getName().toUpperCase(), position);
//		    	newTuple.getTable().columnOrder.put(tl.getTable().tableName.toUpperCase() +"_"+
		    		//	c.getName().toUpperCase(), position);
		    	//position++;
		    }
			for(MyColumn c : tr.getTupleList())
			{
				newTuple.AddColumn(c);
//				newTuple.getTable().columnOrder.put(c.getName().toUpperCase(), position);
				//newTuple.getTable().columnOrder.put(tr.getTable().tableName.toUpperCase() +"_"+
		    		//	c.getName().toUpperCase(), position);
		    	//position++;
			}
			//t.addNewTuple(newTuple);

		}
		
		return newTuple;
	}
	public String toString()
	{
		String out = "";
		for(MyColumn c: tuple)
			out += c.getValue() + " | ";
		return out;
	}
	
	public int getColumnPosition(String name){
		return this.getTable().columnOrder.get(name.toUpperCase());
	}
	
	@Override
	public int compareTo(MyTuple o) {
		for(Integer columnPosition: orderByColumns){
			String stringVal1 = this.getColumn(columnPosition).getValue();
			String stringVal2 = o.getColumn(columnPosition).getValue();
			if(this.getColumn(columnPosition).isInteger() ||
					this.getColumn(columnPosition).isDouble()){
				if(stringVal1.length() > stringVal2.length()){
					int diff = stringVal1.length() - stringVal2.length();
					while(diff > 0){
						stringVal2 = "0" + stringVal2;
						diff--;
					}
				}else if(stringVal1.length() < stringVal2.length()){
					int diff = stringVal2.length() - stringVal1.length();
					while(diff > 0){
						stringVal1 = "0" + stringVal1;
						diff--;
					}
				}
			}
			int val = stringVal1.compareTo(stringVal2);
			if  (val != 0){
				return val;
			}
		}
		return 0;
	}

	/** This method will check to see if a tuple's group by columns are the same so that
	 *  they can be put into groups accordingly.
	 * 
	 * @param t the next tuple to compare to
	 * @return a boolean denoting whether the called upon tuple's group by columns
	 *         are equal to the next tuple's group by columns.
	 */
	public boolean groupByColumnCheck(MyTuple t){
		for(Integer col: groupByColumns){
			String stringVal1 = this.getColumn(col).getValue();
			String stringVal2 = t.getColumn(col).getValue();
			if(this.getColumn(col).isInteger() ||
					this.getColumn(col).isDouble()){
				if(stringVal1.length() > stringVal2.length()){
					int diff = stringVal1.length() - stringVal2.length();
					while(diff > 0){
						stringVal2 = "0" + stringVal2;
						diff--;
					}
				}else if(stringVal1.length() < stringVal2.length()){
					int diff = stringVal2.length() - stringVal1.length();
					while(diff > 0){
						stringVal1 = "0" + stringVal1;
						diff--;
					}
				}
			}
			
			int val = this.getColumn(col).getValue().compareTo(t.getColumn(col).getValue());
			if(val != 0){
				return false;
			}
		}
		return true;
	}
	
	public static void setOrderByColumns(ArrayList<Integer> cols){
		orderByColumns = cols;
	}
	
	public static void setGroupByColumns(ArrayList<Integer> cols){
		groupByColumns = cols;
	}
	
	public MyTable getTable() {
		return table;
	}
	public void setTable(MyTable table) {
		this.table = table;
	}
	
	@Override
	public int hashCode() {
		if(this.getTable().hasConjoinedKey()){
			String key = this.getTupleList().get(0).getValue();
			int pos = this.getTable().
					getColumnPosition(this.getTable().getPrimaryIndexes().get(1));
			key += this.getTupleList().get(pos).getValue();
		   	return Integer.parseInt(key);
		}else{
			return Integer.parseInt(this.getColumn(0).getValue());
		}
	}
	
}
