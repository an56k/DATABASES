package edu.buffalo.cse562;

import java.sql.Date;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class MyExpressionVisitor implements ExpressionVisitor {

	   boolean result;
	     double accumulator;
	     String str;
	     MyColumn col;
	     MyTuple mtup;
	     //later use'
	     static boolean isLong = false, isDouble= false, isDate= false;
	
	public MyExpressionVisitor(MyTuple tup) {
		mtup = tup;
	}
	public boolean getBooleanResult()
	{
		return result;
	}
	public double getAccumulator()
	{
		return accumulator;
	}
	@Override
	public void visit(NullValue stmt) {
		// TODO Auto-generated method 

	}

	@Override
	public void visit(Function stmt) {
		// this one is for AVG, COUNT, SUM, MIN, MAX, Dates
		if(stmt.getName().toUpperCase().equals("DATE"))
		{
			Expression e = (Expression)stmt.getParameters().getExpressions().get(0);
			e.accept(this);
		}
		

	}
	

	@Override
	public void visit(InverseExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue stmt) {
		accumulator = stmt.getValue();
		isDouble = true; isLong = false; isDate = false;
		str = Double.toString(accumulator);
	}

	@Override
	public void visit(LongValue stmt) {
		accumulator = stmt.getValue();
		isDouble = false; isLong = true; isDate = false;
		//str = Long.toString(stmt.getValue());
		str = Double.toString(accumulator);
	}

	@Override
	public void visit(DateValue stmt) {
		Date d = stmt.getValue();
		//str = d.toString();
		str = String.format("%04d-%02d-%02d", d.getYear(), d.getMonth(), d.getDay());
		

	}

	@Override
	public void visit(TimeValue stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis stmt) {
		stmt.getExpression().accept(this); 

	}

	@Override
	public void visit(StringValue stmt) {
		str = stmt.getValue();

		if(str.length() > 4){
			if(str.charAt(4)=='-')
			{
				
				str = String.format("%04d-%02d-%02d", Integer.parseInt(str.substring(0, 4)), Integer.parseInt(str.substring(5, str.indexOf('-',5))), Integer.parseInt(str.substring(str.indexOf('-',5)+1, str.length())));
			}
		}
			
	}

	@Override
	public void visit(Addition stmt) {

        stmt.getLeftExpression().accept(this);
        double leftValue = accumulator;
        stmt.getRightExpression().accept(this);
        double rightValue = accumulator;
        accumulator=(leftValue+rightValue);
        str = Double.toString(accumulator);

	}

	@Override
	public void visit(Division stmt) {
		stmt.getLeftExpression().accept(this);
        double leftValue = accumulator;
        stmt.getRightExpression().accept(this);
        double rightValue = accumulator;
        if(rightValue != 0)
        	accumulator=(leftValue/rightValue);
        str = Double.toString(accumulator);

	}

	@Override
	public void visit(Multiplication stmt) {
		stmt.getLeftExpression().accept(this);
        double leftValue = accumulator;
        stmt.getRightExpression().accept(this);
        double rightValue = accumulator;
        accumulator=(leftValue*rightValue);
        str = Double.toString(accumulator);
	}

	@Override
	public void visit(Subtraction stmt) {
		 stmt.getLeftExpression().accept(this);
	        double leftValue = accumulator;
	        stmt.getRightExpression().accept(this);
	        double rightValue = accumulator;
	        accumulator=(leftValue-rightValue);
	        str = Double.toString(accumulator);

	}

	@Override
	public void visit(AndExpression stmt) {
		stmt.getLeftExpression().accept(this);
		boolean le = result;
		if(le)
		{
			stmt.getRightExpression().accept(this);
			boolean re = result;
			result = re;
		}
		

	}

	@Override
	public void visit(OrExpression stmt) {
		stmt.getLeftExpression().accept(this);
		boolean le = result;
		if(!le)
		{
			stmt.getRightExpression().accept(this);
			boolean re = result;
			result = re;
		}
		

	}

	@Override
	public void visit(Between stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(EqualsTo stmt) {
		stmt.getLeftExpression().accept(this);
		double d1 = 0.0;
		double d2 = 0.0;
		if(isLong || isDouble){
			d1= accumulator;
		}
		String s1 = str;
		stmt.getRightExpression().accept(this);
		if(isLong || isDouble){
			d2 = accumulator;
		}
		if(isDouble || isLong){
			result = (d1 == d2);
		}else{
			String s2 = str;
			result = s1.equalsIgnoreCase(s2);
		}
	}

	@Override
	public void visit(GreaterThan stmt) {
		stmt.getLeftExpression().accept(this);
		String s1="",s2=""; double d1=0, d2=0;
		if(isDouble || isLong)
		{
			d1  = accumulator;
		}
		else
		{
		     s1 = str;
		}
		stmt.getRightExpression().accept(this);
		if(isDouble || isLong)
		{
			d2  = accumulator;
		}
		else
		{
		     s2 = str;
		}
		if(isDouble||isLong)
		{
			result = (d1>d2);
		}
		else{
    	    result = s1.compareTo(s2)>0 ? true: false;
		}

	}

	@Override
	public void visit(GreaterThanEquals stmt) {
		stmt.getLeftExpression().accept(this);
		String s1="",s2=""; double d1=0, d2=0;
		if(isDouble || isLong)
		{
			d1  = accumulator;
		}
		else
		{
		     s1 = str;
		}
		stmt.getRightExpression().accept(this);
		if(isDouble || isLong)
		{
			d2  = accumulator;
		}
		else
		{
		     s2 = str;
		}
		if(isDouble||isLong)
		{
			result = (d1>=d2);
		}
		else{
			result = s1.equals(s2) || (s1.compareTo(s2) > 0 ? true : false);
		}

		

	}

	@Override
	public void visit(InExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(MinorThan stmt) {
		stmt.getLeftExpression().accept(this);
		String s1="",s2=""; double d1=0, d2=0;
		if(isDouble || isLong)
		{
			d1  = accumulator;
		}
		else
		{
		     s1 = str;
		}
		stmt.getRightExpression().accept(this);
		if(isDouble || isLong)
		{
			d2  = accumulator;
		}
		else
		{
		     s2 = str;
		}
		if(isDouble||isLong)
		{
			result = (d1<d2);
		}
		else{
    	    result = s1.compareTo(s2)>=0 ? false: true;
		}
		

	}

	@Override
	public void visit(MinorThanEquals stmt) {
		stmt.getLeftExpression().accept(this);
		String s1="",s2=""; double d1=0, d2=0;
		if(isDouble || isLong)
		{
			d1  = accumulator;
		}
		else
		{
		     s1 = str;
		}
		stmt.getRightExpression().accept(this);
		if(isDouble || isLong)
		{
			d2  = accumulator;
		}
		else
		{
		     s2 = str;
		}
		if(isDouble||isLong)
		{
			result = (d1<=d2);
		}
		else{
			result = s1.equals(s2) || (s1.compareTo(s2) >= 0 ? false : true);
		}
		

	}

	@Override
	public void visit(NotEqualsTo stmt) {
		stmt.getLeftExpression().accept(this);
		String s1="",s2=""; double d1=0, d2=0;
		if(isDouble || isLong)
		{
			d1  = accumulator;
		}
		else
		{
		     s1 = str;
		}
		stmt.getRightExpression().accept(this);
		if(isDouble || isLong)
		{
			d2  = accumulator;
		}
		else
		{
		     s2 = str;
		}
		if(isDouble||isLong)
		{
			result = (d1!=d2);
		}
		else{
			result = ! (s1.equals(s2));
		}
	

	}

	@Override
	public void visit(Column stmt) { 
		if(!(stmt.getColumnName().equalsIgnoreCase(stmt.getWholeColumnName()))){
			int pos = mtup.getTable().getColumnPosition(stmt.getTable().getName().toUpperCase() +"_"+ 
						stmt.getColumnName().toUpperCase());
			col = mtup.getColumn(pos);
		}else{
			int pos = mtup.getColumnPosition(stmt.getColumnName());
			col = mtup.getColumn(pos);
		}
	    str = col.getValue();
	    if(col.isInteger()){
	    	accumulator = col.getIntegerValue(); isLong = true; isDouble = false; isDate = false;}
	    else if(col.isDouble()){
	    	accumulator = col.getDoubleValue(); isLong= false; isDouble = true; isDate = false;}
	    else
	    {
	    	isLong = false; isDouble = false; //System.out.println(str);
	    }

	}

	@Override
	public void visit(SubSelect stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr stmt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor stmt) {
		// TODO Auto-generated method stub

	}
	
	public void setTuple(MyTuple t){
		this.mtup = t;
	}

}
