package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class HashJoin implements Operator {
	
	Operator op1, op2;
    String joinAttr;
    HashMap<String, ArrayList<MyTuple>> hsop;
    //static int index = 0; 
	boolean isCollection = false;
	ArrayList<MyTuple> tempCol;
	MyTuple potential = null ;
	Iterator<MyTuple> iter;
    int frstCnTpl = 0;
    MyTable schema = new MyTable(null,null);
    public HashJoin(Operator op1, Operator op2, String joinAttr)
    {
    	this.op1 = op1;
    	this.op2 = op2;
    	this.joinAttr = joinAttr.toUpperCase();
    	hsop = new  HashMap<String, ArrayList<MyTuple>>();
    	
    			/* Now we will build a hash index on the smaller relation
    	 * op1 will be always the smaller relation
    	 */
    	/*if(op2.getTuples().size() < op1.getTuples().size())
    	{
    		Operator temp = op1;
    		int tpos = op1pos;
    		op1pos = op2pos;
    		op2pos = tpos;
    		op1 = op2;
    		op2 = temp;
    	}*/
    	/*now build the hash*/
    	//op2.resetStream();
    	while(op2.hasNext())
    	{
    		MyTuple t = op2.readOneTuple();
    		int pos = t.getTable().columnOrder.get(joinAttr.toUpperCase());
    		MyColumn col = t.getColumn(pos);
    		String key = col.getValue();
    		if(hsop.containsKey(key))
    		{
    			ArrayList<MyTuple> temp = hsop.get(key);
    			temp.add(t);
    			hsop.put(key, temp);
    		}
    		else
    		{
    			ArrayList<MyTuple> temp = new ArrayList<MyTuple> (1);
    			temp.add(t);
    			hsop.put(key, temp);
    		}
    		
    	}
    			
    }
	@Override
	public void resetStream() {
		op1.resetStream();  //I'm not sure , ??? ask Alex

	}

	@Override
	public MyTuple readOneTuple() {
		if(isCollection)
		{
			MyTuple t = readFromBucket();
			if(t!=null){
				MyTuple tmp= MyTuple.ConcatTuple(potential,t);
				tmp.setTable(schema);
				return tmp;
			}
			
		}
		if(op1.hasNext())
		{
			do{
				potential = op1.readOneTuple();
				if(potential != null)   //when one of the tables is empty
				{
					int pos = potential.getTable().getColumnPosition(joinAttr); 
					MyColumn col = potential.getColumn(pos);
					String key = col.getValue();
					if(hsop.containsKey(key))
					{
						tempCol = hsop.get(key);
						iter = tempCol.iterator();
						if(iter.hasNext())
						{
							tempCol = hsop.get(key);
							iter = tempCol.iterator();
							if(iter.hasNext())
							{
								isCollection = true;
								MyTuple t =  readFromBucket();
								frstCnTpl ++;
								if(frstCnTpl == 1)
								{
									schema = MyTuple.concatSchema(potential, t);
								}
									
								MyTuple tmp= MyTuple.ConcatTuple(potential,t);
								tmp.setTable(schema);
								return tmp;
							}
							
						}
					}else{
						return null;
					}
				}
			}while(op1.hasNext());
			frstCnTpl = 0;
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		
		return op1.hasNext();
	}

	@Override
	public List<MyTuple> getTuples() {
		// TODO Auto-generated method stub
		return null;
	}
	private MyTuple readFromBucket()
	{
		
		if(iter.hasNext()){
			MyTuple t = iter.next();
			return t;
		}
		else{
		    isCollection = false;
		}
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
		return (op1.getSize() + op2.getSize());
	}

}
