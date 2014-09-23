package edu.buffalo.cse562;

import java.util.List;

public interface Operator {
	public void resetStream();
	
	public MyTuple readOneTuple();
	
	public MyTuple readOneTupleFromFile();
	
	public boolean hasNextFromFile();
	
	public boolean hasNext();
	
	public List<MyTuple> getTuples();
	
	public String getTablename();
	
	public int getSize();
}
