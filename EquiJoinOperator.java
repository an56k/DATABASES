package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class EquiJoinOperator implements Operator {

	Operator left;
	Operator right;
	Expression condition;
	MyTuple leftVal;
	int i;
	
	public EquiJoinOperator(Operator left, Operator right, Expression exp)
	{
		this.left= left;
		this.right = right;
		condition = exp;
		this.left.resetStream();
		this.right.resetStream();
		leftVal = left.readOneTuple();
		i = 1;
	}
	@Override
	public void resetStream() {
		left.resetStream(); 
		right.resetStream();
	}

	@Override
	public MyTuple readOneTuple() {
		MyTuple joinTuple = null;
							
		if(!right.hasNext()) {
			right.resetStream(); 
			leftVal = left.readOneTuple();
		}
		while(right.hasNext()){
			MyTuple tr = right.readOneTuple();
			joinTuple = MyTuple.ConcatTuple(leftVal, tr);
			MyExpressionVisitor me = new MyExpressionVisitor(joinTuple);
			condition.accept(me);
			if(me.getBooleanResult()){
			      return joinTuple;
			}else{
				 right.resetStream();
				 leftVal = left.readOneTuple();
				 System.out.println(i);
				 i++;
			}
		}						
		return null; 	
	
			
	}
	
	@Override
	public boolean hasNext() {
		return left.hasNext() && right.hasNext();
	}

	@Override
	public List<MyTuple> getTuples() {
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
