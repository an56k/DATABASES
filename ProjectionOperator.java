package edu.buffalo.cse562;

import java.util.List;

public class ProjectionOperator implements Operator{
    /** A list of  columns we are dealing with (one tuple)*/
	List<Integer> cols;
    
    Operator input;
    
	public ProjectionOperator(List<Integer> cols, Operator input)
	{
		this.cols = cols;
		this.input = input;
		resetStream();
	}
	public void resetStream() {
		input.resetStream();
		
	}

	
	public MyTuple readOneTuple() {
		
		if(input.hasNext()){
	       MyTuple result = new MyTuple(null);
	       
	       MyTuple temp = input.readOneTuple();
	       for(int i=0; i<cols.size(); i++)
	       {
	    	   result.AddColumn(temp.getColumn(cols.get(i)));
	       }
	       return result;
	    }
		return null;
		
	}
	
	public boolean hasNext() {
		return input.hasNext();
	}
	@Override
	public List<MyTuple> getTuples() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public MyTuple readOneTupleFromFile() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean hasNextFromFile() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public String getTablename() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		return 0;
	}

}
