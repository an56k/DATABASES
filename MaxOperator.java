package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class MaxOperator implements Operator{
    Operator input;
    Expression exp;
    public MaxOperator(Operator in, Expression exp)
    {
    	input = in;
    	this.exp=exp;
    }
    public double findMax(){
        this.resetStream();
        double max;
       
         MyTuple t = this.readOneTuple();
         MyExpressionVisitor arith = new MyExpressionVisitor(t);
       	max = arith.getAccumulator();
        
        while(this.hasNext())
        {
             t = this.readOneTuple();
             arith =new MyExpressionVisitor(t);
          	exp.accept(arith);
        	double d = arith.getAccumulator();
        	if(max < d)
        		max = d;
        }
        return max;
    }
	@Override
	public void resetStream() {
		input.resetStream();
		
	}

	@Override
	public MyTuple readOneTuple() {
		if(hasNext())
     		return input.readOneTuple();
		return null;
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
