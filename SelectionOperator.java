package edu.buffalo.cse562;

import java.util.Collection;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class SelectionOperator implements Operator {

	/** The condition to be evaluated for each relation */
	Expression condition;
	
	/** The data to be traversed */
	Operator input;
	
	/** A simple constructor */
	public SelectionOperator(Expression cond, Operator input){
		condition = cond;
		this.input = input;
		//resetStream();
	}
	
	@Override
	public void resetStream() {
		input.resetStream();
	}

	@Override
	public MyTuple readOneTuple() {
		MyTuple next = input.readOneTuple();

		if(next == null)
			
			return null; 
		if(condition == null)
			return next;

	
		/* boolean kept to determine if a tuple meets the condition */
		boolean meetsWhereClause = false;
		
		/* evaluate where clause for the tuple */
		MyExpressionVisitor be = new MyExpressionVisitor(next);
		condition.accept(be);
		meetsWhereClause = be.getBooleanResult();
		if(meetsWhereClause){
			return next;
		}else{
			return null;
		}
	}

	@Override
	//Selection will be done before anthing so it is always reading from a file
	public boolean hasNext() {
		return input.hasNextFromFile();
	}

	@Override
	public List<MyTuple> getTuples() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MyTuple readOneTupleFromFile() {
		MyTuple next = input.readOneTupleFromFile();

		if(next == null)
			
			return null; 
		if(condition == null)
			return next;

	
		/* boolean kept to determine if a tuple meets the condition */
		boolean meetsWhereClause = false;
		
		/* evaluate where clause for the tuple */
		MyExpressionVisitor be = new MyExpressionVisitor(next);
		condition.accept(be);
		meetsWhereClause = be.getBooleanResult();
		if(meetsWhereClause){
			return next;
		}else{
			return null;
		}
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
