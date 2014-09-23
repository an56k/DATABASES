
package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class SumOperator implements Operator {

	 Operator input;
	 Expression exp;
	 
	 public SumOperator(Operator in, Expression exp)
	 {
		 input = in;
		 this.exp = exp; 
	 }
	 public double sum()
	 {
		 double sum = 0;
		 resetStream();
		 while(this.hasNext())
		 {
			 MyTuple t  = this.readOneTuple();
			 MyExpressionVisitor arith = new MyExpressionVisitor(t);
			 exp.accept(arith);
			 sum += arith.getAccumulator();
		 }
		 return Math.round(sum);
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
