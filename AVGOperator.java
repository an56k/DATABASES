package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class AVGOperator implements Operator {
    
	Operator input;
	Expression exp;
	public AVGOperator(Operator in, Expression exp)
	{
		input = in;
		this.exp = exp;
	}
	public double average()
	{
		double sum = 0;
		int count = 0;
		this.resetStream();
		while(this.hasNext())
		{
			count++;
			MyTuple temp =this.readOneTuple();
		    MyExpressionVisitor arith = new MyExpressionVisitor(temp);
		    exp.accept(arith);
		    double d = arith.getAccumulator();
		    sum += d;
		    
		}
		return (sum/count);
	}
	@Override
	public void resetStream() {
		input.resetStream();

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
