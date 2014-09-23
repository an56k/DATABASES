package edu.buffalo.cse562;

import java.util.List;

public class CartesianProduct implements Operator{

	Operator left;
	Operator right;
	MyTuple leftVal;
	
	public CartesianProduct(Operator left, Operator right)
	{
		this.left = left;
		this.right = right;
		//left.resetStream();
		//right.resetStream();
		leftVal = null;
		while(leftVal == null){
		   leftVal = left.readOneTuple();
		}
		
	}
	@Override
	public void resetStream() {
		left.resetStream();
		right.resetStream();
	}

	@Override
	public MyTuple readOneTuple() {
		MyTuple joinTuple = null;
		if(!right.hasNext()){ 
			if(left.hasNext()){
				right.resetStream();
                	  leftVal = left.readOneTuple();
                       
	
			}else{
				return null;
			}
		}else{
			MyTuple tr = right.readOneTuple();
			joinTuple = MyTuple.ConcatTuple(leftVal, tr);			
		}

		return joinTuple;
	}

	@Override
	public boolean hasNext() {
       return left.hasNext() || right.hasNext();
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
