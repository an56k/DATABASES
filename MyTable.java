package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import jdbm.PrimaryStoreMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;




public class MyTable implements Operator {
	
	List<MyTuple> rows;
	FileReader fr;
	BufferedReader bf;
	Iterator<MyTuple> it;  //int i=0;
	String nextTuple = null;
	int i;
	boolean isReadingFromFile= false;
	
	/** The index directory where the heap file will be stored */
	private String indexDir;

	/** The name of this table's data file on disk */
	private File dataFile;
	
	/** ALEX: This hash map denotes the order in which columns appear in the talble, hashed by
	 * column name. */
	HashMap<String, Integer> columnOrder;
	
	/** ALEX: This is the name of the table */
	String tableName; 
	
	/**LADAN: This is the alias of the table*/
	String alias;
	
	 /** ALEX: This hash denotes the type of each column */
	HashMap<String, String> columnTypes;
	
	/** ALEX: we need a list of the column names in the same order they appear in the schema */
	ArrayList<String> columnList;
	
	/**Ladan: we need a lost of indexed attribute */
	private ArrayList<String> PrimaryIndexes;
	private ArrayList<String> SecondaryIndexes;
	
	public MyTable(String tableName, HashMap<String, Integer> cols, List<MyTuple> tbl, 
			HashMap<String, String> colTypes, String indexDir)
	{
		this.tableName= tableName; 
		columnOrder = cols;
		rows = tbl;
		//it = rows.iterator();
		i=0;
		columnTypes = colTypes;
		this.indexDir = indexDir;
		
	}
	public MyTable(List<MyTuple> tbl, String indexDir)
	{ 
		rows = tbl;
		columnOrder = new HashMap<String, Integer>();
		//it = rows.iterator();
		i=0;
		this.indexDir = indexDir;
	}
	
	/** ALEX: A constructor that will be used when we do not want to load every table
	 *  into memory. This will set the a pointer to the .dat file so that we can only
	 *  load the tables into memory that we need.
	 */
	public MyTable Clone()
	{
		MyTable newtbl = new MyTable(tableName,columnOrder,rows, columnTypes, indexDir);
		newtbl.dataFile = this.dataFile;
		newtbl.columnList = this.columnList;
		newtbl.setPrimaryIndexes(this.getPrimaryIndexes());
	newtbl.setSecondaryIndexes(this.getSecondaryIndexes());
		return newtbl;
	}
	public MyTable(File dat, String tableName, HashMap<String, Integer> cols, 
			HashMap<String, String> colTypes, ArrayList<String> columnList){
		dataFile = dat;
		this.tableName= tableName; 
		columnOrder = cols;
		columnTypes = colTypes;
		this.columnList = columnList;

	}
	public MyTable(File dat, String tableName, HashMap<String, Integer> cols, 
			HashMap<String, String> colTypes, ArrayList<String> columnList, ArrayList<String> pind, ArrayList<String> sind){
		dataFile = dat;
		this.tableName= tableName; 
		columnOrder = cols;
		columnTypes = colTypes;
		this.columnList = columnList;
		this.PrimaryIndexes = pind;
		this.SecondaryIndexes = sind;

	}
	
	public void resetStream() {
	    i=0;
	    if(dataFile!= null){
		    try{
			    fr = new FileReader(dataFile);
			    bf = new BufferedReader(fr);
			}
		    catch(IOException ex)
			{
				ex.printStackTrace();
			}
	    }

		
		//it = rows.iterator();
	}
	/* Ladan:This method is called when we push down selections and want to
	 * read tables from File for the first time
	 * 
	 */
	public MyTuple readOneTupleFromFile()
	{
		
          if(nextTuple != null)
            {
			
				String[] values = nextTuple.split("\\|");
				MyTuple newTuple = new MyTuple(this);
				int j = 0;
				for(String name: columnList){
					boolean isDouble = false;
					boolean isInteger = false;
					String nextType = columnTypes.get(name);
					if(nextType.equalsIgnoreCase("float")||nextType.equalsIgnoreCase("DECIMAL")){
						isDouble = true;
					}else if(nextType.equalsIgnoreCase("int")){
						isInteger = true;
					}
					MyColumn col;
					//if(alias == null)
				    col= new MyColumn(name.toUpperCase(), values[j], isDouble, isInteger, tableName);
					//else
					//	col= new MyColumn(name.toUpperCase(), values[j], isDouble, isInteger, alias);
					newTuple.AddColumn(col);
					j++;
				}
				return newTuple;
			
		    }
	
	      return null; // this indicates end of the file
		
	}
	public MyTuple readOneTuple() {
		if(hasNext())
		{
		
			MyTuple temp = rows.get(i++);
			return temp;
		}
		return null; // null or exception???
		
	}
	@Override
	public boolean hasNext() {
		return i<rows.size();
	}
	 
	
	
	/** ALEX: this method will return the position of a column in
	 *  a table based on the name of the column */
	public int getColumnPosition(String colName){
	/*	if(colName.contains("_"))
		{
			colName = colName.substring(colName.indexOf("_")+1);
		}*/
		return columnOrder.get(colName.toUpperCase());
	}
	
	/** This method will add a new tuple to the table
	 * @param newTuple the new tuple to be added
	 */
	public void addNewTuple(MyTuple newTuple){
		rows.add(newTuple);
	}
	@Override
	public List<MyTuple> getTuples() {
		return rows;
	}
	
	@Override
	public String getTablename(){
		return tableName;
	}
	
	public void setRows(ArrayList<MyTuple> rows) {
		this.rows = rows;
	}
	public void setName(String tableName) {
		this.tableName = tableName;
	}
	
	public void setColumnOrder(HashMap<String, Integer> columnOrder) {
		this.columnOrder = columnOrder;
	}
	
	public void setColumnTypes(HashMap<String, String> colTypes) {
		columnTypes = colTypes;
		
	}
		
		/** ALEX: This method will load all the data from the .dat file (dataFile field)
		 *  into memory for use with this instance of MyTable
		 */
		public void loadData() {
				FileReader tableDataStream;
				try {
					tableDataStream = new FileReader(dataFile);
					BufferedReader dataReader = new BufferedReader(tableDataStream);
					String tupleString;
					ArrayList<MyTuple> tuples = new ArrayList<MyTuple>();
					while((tupleString = dataReader.readLine()) != null){
						String[] values = tupleString.split("\\|");
						MyTuple newTuple = new MyTuple(this);
						int j = 0;
						for(String name: columnList){
							boolean isDouble = false;
							boolean isInteger = false;
							String nextType = columnTypes.get(name);
							if(nextType.equalsIgnoreCase("float")||nextType.equalsIgnoreCase("DECIMAL")){
								isDouble = true;
							}else if(nextType.equalsIgnoreCase("int")){
								isInteger = true;
							}
							MyColumn col;
								
						    col= new MyColumn(name.toUpperCase(), values[j], isDouble, isInteger, tableName);
						
							newTuple.AddColumn(col);
							j++;
						}
						tuples.add(newTuple);
					}
					this.setRows(tuples);
					dataReader.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}catch (IOException e){
					e.printStackTrace();
				}
	}
		
		/** When we have all of our data in a heap file, we need
		 *  to use this method to load data.
		 * 
		 */
		public void loadDataFromHeapFile(){
			RecordManager recman;
			try {
				recman = RecordManagerFactory.createRecordManager(indexDir.toString() + 
						File.separatorChar + this.getTablename());
				PrimaryStoreMap<Long, MyTuple> index = recman.storeMap("Primary_" +this.getTablename(), 
						new MyTupleSerializer(this.columnList.size(), this));
				/* Instantiate rows */
				this.rows = new ArrayList<MyTuple>();
				for(MyTuple t: index.values()){
					this.rows.add(t);
				}
				recman.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
		@Override
		public boolean hasNextFromFile() {
			// TODO Auto-generated method stub
			if(fr== null && bf == null)
			{
				try{
				    fr = new FileReader(dataFile);
				    bf = new BufferedReader(fr);
				}
			    catch(IOException ex)
				{
					ex.printStackTrace();
				}
			}
			
		try {
				if((nextTuple=bf.readLine())!=null)
					return true;
				else
					bf.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}	
		public ArrayList<String> getPrimaryIndexes()
		{
			return PrimaryIndexes;
		}
		public ArrayList<String> getSecondaryIndexes()
		{
			return SecondaryIndexes;
		}
		public boolean hasPrimaryIndexOn(String att)
		{
			for(int i=0; i<PrimaryIndexes.size(); i++)
			{
				if(PrimaryIndexes.get(i).equalsIgnoreCase(att))
					return true;
			}
			return false;
		}
		public boolean hasSecondaryIndexOn(String att)
		{
			for(int i=0; i<SecondaryIndexes.size(); i++)
			{
				if(SecondaryIndexes.get(i).equalsIgnoreCase(att))
					return true;
			}
			return false;
		}
		
		public boolean hasConjoinedKey(){
			return this.PrimaryIndexes.size() > 1;
		}
		
		public void setColumnList(ArrayList<String> list){
			this.columnList = list;
		}
		
		public void setPrimaryIndexes(ArrayList<String> indexes){
			this.PrimaryIndexes = indexes;
		}

		public void setSecondaryIndexes(ArrayList<String> indexes){
			this.SecondaryIndexes= indexes;
		}
		
		@Override
		public int getSize() {
			return rows.size();
		}
		
		public void setIndexDir(String dir){
			this.indexDir = dir;
		}
}
