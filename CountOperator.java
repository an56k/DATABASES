package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class CountOperator implements Operator{
    Operator input;
    boolean isDistinct; 
    Expression exp;
    
    public CountOperator(Operator in, Expression ex, boolean isDis)
    {
    	input = in;
    	exp = ex;
    	isDistinct = isDis;
    }
    public CountOperator(Operator in, boolean isDis)
    {
    	input = in; 
    	isDistinct = isDis;
    }
	@Override
	public void resetStream() {
	      input.resetStream();
		
	}
	public int getCount()
	{
		int count =0;
		if(!isDistinct){
			
			resetStream();
			
			while(this.hasNext())
			{
				MyTuple t = this.readOneTuple();
				
				count ++;
			}
			
		}
		else
		{
			HashMap<String, Integer> hscount = new HashMap<String, Integer>();
			resetStream();
			while(this.hasNext())
			{
				MyTuple t = this.readOneTuple();
				Column ctmp = (Column)exp;
				int pos = t.getColumnPosition(ctmp.getColumnName().toUpperCase());
				String key = t.getColumn(pos).getValue();
				if(!hscount.containsKey(key))
				{
					count++;
					hscount.put(key, 1);
				}
				
			}
			
		}
		return count;
	}

	@Override
	public MyTuple readOneTuple() {
		return input.readOneTuple();
	}

	@Override
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
