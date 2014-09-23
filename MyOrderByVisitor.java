package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;

public class MyOrderByVisitor implements OrderByVisitor{

	/** A collection of existing tuples */
	Operator tupleList;
	
	/** Some columns to order by */
	ArrayList<OrderByElement> orderByElements;
	
	/** The positions of the columns to order by */
	ArrayList<Integer> columnPositions;
	
	public MyOrderByVisitor(Operator tupleList, 
			ArrayList<OrderByElement> orderByElements, ArrayList<Integer> pos) {
		this.tupleList = tupleList;
		this.orderByElements = orderByElements;
		columnPositions = pos;
	}
	
	public Operator initialize(){
		MyTuple.setOrderByColumns(columnPositions);
		for(OrderByElement s: this.orderByElements){
			s.accept(this);
		}
		return tupleList;
	}

	@Override
	public void visit(OrderByElement arg0) {
		if(arg0.isAsc()){
			Collections.sort(tupleList.getTuples());
		}else{
			Collections.sort(tupleList.getTuples(), Collections.reverseOrder());
		}
	}

}
