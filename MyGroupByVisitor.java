package edu.buffalo.cse562;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

public class MyGroupByVisitor {
	/** a collection of tuples */
	Operator tuples;
	
	/** a collection of group by elements */
	ArrayList<Integer> groupByElements;
	
	/** a collection of aggregate functions */
	ArrayList<Expression> aggregateFunctions;
	
	/** a collection of projected columns */
	ArrayList<Integer> projections;
	
	/** The alias' for all the aggregates */
	ArrayList<String> aggregatesAlias;
	
	
	public MyGroupByVisitor(Operator tuples, ArrayList<Integer> groupByElements,
			ArrayList<Expression> aggregateFunctions, ArrayList<Integer> projects, 
			ArrayList<String> aggregatesAlias){
		this.tuples = tuples;
		this.groupByElements = groupByElements;
		this.aggregateFunctions = aggregateFunctions;
		this.projections = projects;
		this.aggregatesAlias = aggregatesAlias;

	}
	
	public Operator initialize(){
		if(groupByElements.size() == 0){
			MyTable newTable = new MyTable(new ArrayList<MyTuple>(),null);
			MyTuple t = new MyTuple(newTable);
			
			/* run each aggregate on the entire collection of tuples */
			int i = 0;
			for(Expression e: aggregateFunctions){
				Function f = (Function) e;
				t.AddColumn(evaluateAggregateFunction(tuples, f, aggregatesAlias.get(i)));
			}
		    ArrayList<MyTuple> newTuples = new ArrayList<MyTuple>();
		    newTuples.add(t);
		    newTable.setRows(newTuples);
		    return newTable;
		}else{
			/* create groups */
			ArrayList<MyTable> groups = new ArrayList<MyTable>();
			MyTuple.setOrderByColumns(groupByElements);
			MyTuple.setGroupByColumns(groupByElements);
			Collections.sort(tuples.getTuples()); 
			MyTuple previousTuple = null;
			MyTuple nextTuple = null;
			MyTable nextGroup = new MyTable(new ArrayList<MyTuple>(),null);
			int iteration = 1;
			for(MyTuple t: tuples.getTuples()){
				if(iteration != 1){
					nextTuple = t;
					if(nextTuple.groupByColumnCheck(previousTuple) == false){
						groups.add(nextGroup);
						nextGroup = new MyTable(new ArrayList<MyTuple>(),null);
					}
				}
				previousTuple = t;
				nextGroup.addNewTuple(previousTuple);
				iteration++;
			}
			/* add the last group to the collection of groups */
			groups.add(nextGroup);
			
			/* eval all aggregate functions for each group */
			ArrayList<MyColumn> aggregateColumns = null;
			ArrayList<MyTuple> newTuples = new ArrayList<MyTuple>();
			
			/* keep track of the new column positions */
			HashMap<String , Integer> colPositions = new HashMap<String, Integer>();
			
			MyTable newTable = new MyTable(new ArrayList<MyTuple>(),null);
			for(MyTable table: groups){
				aggregateColumns = new  ArrayList<MyColumn>();
				int i = 0;
				for(Expression e: aggregateFunctions){
					Function f = (Function) e;
					aggregateColumns.add(evaluateAggregateFunction(table, f, 
							aggregatesAlias.get(i)));
				}
				/* create new tuple with the projections and the aggregates in order */
				int aggColIndex = 0;
				int colPosition = 0;
				MyTuple tuple = new MyTuple(newTable);

				for(Integer col: projections){
					if(col == -1){
						tuple.AddColumn(aggregateColumns.get(aggColIndex));
						colPositions.put(aggregateColumns.get(aggColIndex).getName(),
								colPosition);
						colPositions.put(aggregateColumns.get(aggColIndex).getTableName().toUpperCase()
								+"_"+aggregateColumns.get(aggColIndex).getName(),
								colPosition);
						aggColIndex++;
					}else{
						tuple.AddColumn(table.rows.get(0).getColumn(col));
						colPositions.put(table.rows.get(0).getColumn(col).getName(),
								colPosition);
						colPositions.put(table.rows.get(0).getColumn(col).getTableName().toUpperCase() 
								+"_"+ table.rows.get(0).getColumn(col).getName(),
								colPosition);
						if(table.rows.get(0).getColumn(col).getAlias()!=null)
						{
							colPositions.put(table.rows.get(0).getColumn(col).getAlias().toUpperCase(),colPosition);
							colPositions.put(table.rows.get(0).getColumn(col).getTableName().toUpperCase() 
									+"_"+ table.rows.get(0).getColumn(col).getAlias().toUpperCase(),
									colPosition);
						}
					
					}
					colPosition++;
				}
				newTuples.add(tuple);
			}
			newTable.setName("SUBQUERY"); //LADAN I use SUBQUERY instead of ""
			newTable.setColumnOrder(colPositions);
			newTable.setRows(newTuples);
			newTable.setColumnTypes(null);
			return newTable;
		}
	}
	
	/** This function will evaluate the aggregate function (func) for the
	 * 	operator (tuples) and return a column representing the result
	 * 	of the aggregate evaluation.
	 * 
	 * @param tuples the operator to run the aggregate function upon.
	 * @param func the aggregate function.
	 * @return a column containing the aggregate's result
	 */
	public MyColumn evaluateAggregateFunction(Operator op, Function func,
			String alias){
		MyColumn c = null;
		
		if(func.getName().equalsIgnoreCase("AVG")){
			AVGOperator avg = new AVGOperator(op, (Expression) func.getParameters().getExpressions().get(0));
			String val = new DecimalFormat("#0.0000").format(avg.average());
			c = new  MyColumn(alias, val, true, false, "");
		}else if (func.getName().equalsIgnoreCase("SUM")){
			SumOperator avg = new SumOperator(op, (Expression) func.getParameters().getExpressions().get(0));
			String val = Double.toString(avg.sum());
			c = new  MyColumn(alias, val, true, false, "");
		}else if(func.getName().equalsIgnoreCase("COUNT")){
			CountOperator count;
			if(func.isAllColumns())
			{
				count = new CountOperator(op, func.isDistinct());
			}
			else{
			    count = new CountOperator(op, (Expression) func.getParameters().getExpressions().get(0), func.isDistinct());
			}
			String val = Integer.toString(count.getCount());
			c = new MyColumn(alias, val, false, true, "");
		}else if (func.getName().equalsIgnoreCase("MIN")){
			MinOperator min = new MinOperator(op, (Expression) func.getParameters().getExpressions().get(0));
			String val = Double.toString(min.findMin());
			c = new MyColumn(alias, val, true, false, "");
		}else if (func.getName().equalsIgnoreCase("MAX")){
			MaxOperator max = new MaxOperator(op, (Expression) func.getParameters().getExpressions().get(0));
			String val = Double.toString(max.findMax());
			c = new MyColumn(alias, val, true, false, "");
		}
		return c;
	}
}
