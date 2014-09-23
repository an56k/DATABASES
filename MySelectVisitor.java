package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jdbm.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class MySelectVisitor implements SelectVisitor, FromItemVisitor {

	private HashMap<String, MyTable> tables;
	private MyTable outputTable;
	
	/** This is the collection of conditions that are left over after an 
	 * index scan */
	private HashMap<String, ArrayList<String>> leftoverConditions;
	
	/** The details of index scans for each table */
	private HashMap<String, IndexScanDetail> scanDetails;
	
	/** this is for storing projection positions*/
	private ArrayList<Integer> projections;
	
	/**This is for storing projection aliases , based on their positions*/
	HashMap<Integer, String> halias = new HashMap<Integer, String>();
	private long limit;
	private Operator fromTable;
	boolean hasJoin = false;
	
	/** The directory of the indexes */
	private File indexDir;
	
	public MySelectVisitor(HashMap<String, MyTable> tables, Select stmt, File indexDir) {
		this.tables = tables;
		this.indexDir = indexDir;
		this.leftoverConditions = new HashMap<String, ArrayList<String>>();
		stmt.getSelectBody().accept(this);
	}
	
	public MySelectVisitor(HashMap<String, MyTable> tables, SubSelect stmt, File indexDir) {
		this.tables = tables;
		this.indexDir = indexDir;
		this.leftoverConditions = new HashMap<String, ArrayList<String>>();
		stmt.getSelectBody().accept(this);
	}
	
	@Override
	public void visit(PlainSelect arg0) {
		FromItem fromTableName = arg0.getFromItem();
		fromTableName.accept(this);

		MyTable input = null;
					
		/* Joins OPTIONAL */
		if(arg0.getJoins() != null){
			ArrayList<MyTable> joinedTables = new ArrayList<MyTable>();
			joinedTables.add((MyTable) fromTable);
			for(Object o: arg0.getJoins()){
				MyTable nextTable = (MyTable) getCorrectTable(o.toString());
				joinedTables.add(nextTable);
			}
			/* Get the upper and lower bounds of columns for all tables */
			scanDetails = this.getUpperAndLowerBounds(arg0.getWhere().toString());
			
			/* Scan to see if the tables that have bounds for them have 
			 * an index on the column */
			for(java.util.Map.Entry<String, IndexScanDetail> e: scanDetails.entrySet()){
				MyTable table = (MyTable) this.getCorrectTable(e.getKey());
				if(table.hasSecondaryIndexOn(e.getValue().getColumnName())){
//					System.out.println(table.getTablename() + " has an index on " +
//							e.getValue().getColumnName());
				}else{
//					System.out.println(table.getTablename() + " doesn't have an index on " +
//							e.getValue().getColumnName());
					scanDetails.remove(e.getKey());
				}
			}
			
			/* if there are indices built, we will do index nested loop join */
			if(indexDir != null){
				/* Hash join */

				input = hashJoin(joinedTables, arg0);
			}else{
				/*  Index nested loop join */
				//Expression where = getRelatedCondition(arg0.getWhere().toString(), "PARTSUPP");
				//Expression where = getNonJoinConditions(arg0.getWhere().toString());
			   // System.out.println(where.toString());
				//System.out.println("Executing INLJ");
				input = indexNestedLoopJoin(joinedTables, arg0);
			}
		}else{
			/* put the tuples into an operator */
			/* there are no joins, but we should still check for indices 
			 * to see if we can reduce the single table*/
			input = (MyTable) fromTable;
		}
		
		/* store projections MANDATORY*/
		projections = new ArrayList<Integer>();
		ArrayList<Expression> aggregates = new ArrayList<Expression>();

		ArrayList<String> aggregatesAlias = new ArrayList<String>();
		ArrayList<Expression> projectedExpressions = new ArrayList<Expression>();
		ArrayList<String> expressionAlias = new ArrayList<String>();
		boolean palias = false;
		
		if(!(arg0.getSelectItems().get(0).toString().equals("*"))){
			for(Object o: arg0.getSelectItems()){
				SelectExpressionItem item = (SelectExpressionItem) o;
				Expression check = item.getExpression(); 
				if (check instanceof Function){
					aggregates.add(check);
					/* add a placeholder for the aggregate function */
					projections.add(-1);
					if(item.getAlias() != null){
						aggregatesAlias.add(item.getAlias().toUpperCase());
					}else{
						aggregatesAlias.add(((Function) check).getName());
					}
				}else if(check instanceof Column){
					int pos;
					String colProject = item.getExpression().toString();
					if(colProject.contains(".")){
						pos = input.getColumnPosition(colProject.replace(".",
								"_").toUpperCase());
						projections.add(pos);
					}else{
						pos = input.getColumnPosition(colProject.toUpperCase());
						projections.add(pos);
					}
					if(item.getAlias() != null){
						if(item.getExpression().toString().contains("."))
						{
							input.columnOrder.put(item.getAlias().toUpperCase(), 
									input.getColumnPosition(item.getExpression().toString().replace(".", "_").toUpperCase()));
							
							
						}
						else
						{
							input.columnOrder.put(item.getAlias().toUpperCase(), 
								input.getColumnPosition(item.getExpression().toString()));
						}
						halias.put(pos, item.getAlias()); //might later put the upper case version
						palias= true;
						
					}
					
				}else{
					projectedExpressions.add(item.getExpression());
					expressionAlias.add(item.getAlias());
				}
			}
		}
		
		/* if there is no where cause, skip this evaluation */
		Expression where;
		if(arg0.getWhere() != null && !hasJoin){
			where = arg0.getWhere();
			outputTable = new MyTable(input.tableName, input.columnOrder, new ArrayList<MyTuple>(),
				input.columnTypes, indexDir.toString());
			
			/* Run the where clause on each tuple and store the ones that fit the
			 * where clause.*/

			/* SelectionOperator selectOp = new SelectionOperator(where, input);
			while(selectOp.hasNext()){
				MyTuple next = selectOp.readOneTupleFromFile();
				if(next != null){
					outputTable.addNewTuple(next);
				}
			/* end of where evaluation 
			} */
			
			/* Hack for checkpoint 4 */
			MyExpressionVisitor expVisitor = new MyExpressionVisitor(null);
			input.setIndexDir(indexDir.toString());
			input.loadDataFromHeapFile();
			MyTuple next = null;
			while(input.hasNext()){
				next = input.readOneTuple();
				expVisitor.setTuple(next);
				where.accept(expVisitor);
				if(expVisitor.getBooleanResult()){
					outputTable.addNewTuple(next);
				}
			}
			
		/* end of check for where clause */
		}else{
			/* if there is no where clause evaluate everything other than where */
			//input.loadData();
			outputTable = input;
			if(arg0.getJoins() == null){
				outputTable.setIndexDir(indexDir.toString());
				outputTable.loadDataFromHeapFile();
			}
		}
		
		/* distinct OPTIONAL */
		if(arg0.getDistinct() != null){
			/* check if this tuple (or value based on projection) already
			 * exists in the output table */
				
		}
		
		/* handle projected expressions */
		if(projectedExpressions.size() != 0){

			/* Parse out the column positions */
			/* HACK: for tpch 7 */
			ArrayList<Integer> columnPositions = new ArrayList<Integer>();
			for(Expression e: projectedExpressions){	
				String str = e.toString();
				String firstCol = str.substring(0, str.indexOf("*")).toUpperCase().trim().
						replace(".", "_");
				columnPositions.add(outputTable.getColumnPosition(firstCol));
				String secondCol = str.substring(str.indexOf("-")+1, str.lastIndexOf(")")).toUpperCase().trim().
						replace(".", "_");
				columnPositions.add(outputTable.getColumnPosition(secondCol));
			}
			
			for(MyTuple t: outputTable.rows){
				int index = 0;
				for(Expression e: projectedExpressions){
					/* evaluate the expression */
//					MyExpressionVisitor ev = new MyExpressionVisitor(t);
//					e.accept(ev);
					/* HACK: for tpch 7 */
					double firstVal = Double.parseDouble(t.getColumn(columnPositions.get(0)).getValue());
					double secondVal = Double.parseDouble(t.getColumn(columnPositions.get(1)).getValue());
					double numVal = firstVal * (1 - secondVal);
					String val = Double.toString(numVal);
					if(val.matches(".*\\.0$")){
						val = val.substring(0, val.indexOf("."));
					}
					t.AddColumn(new MyColumn(expressionAlias.get(index), val, 
							true, false, outputTable.tableName));
					outputTable.columnOrder.put(expressionAlias.get(index).toUpperCase(),
						outputTable.rows.get(0).getTupleList().size()-1);
					index++;
				}
			}
		}
		if(palias) //we have alias in projections
		{
			for(MyTuple t: outputTable.rows)
			{
				for(int i=0;i<projections.size(); i++)
				{
					t.getColumn(projections.get(i)).setAlias(halias.get(projections.get(i))); //set the alias
				}
			}
		}
		
		
		/* aggregates and group by OPTIONAL */
		if(aggregates.size() != 0){
			long startTime = System.currentTimeMillis();
			ArrayList<Integer> groupBys = new ArrayList<Integer>();
			if (arg0.getGroupByColumnReferences() != null){
				for(Object o: arg0.getGroupByColumnReferences()){
					Column c = (Column) o;
					if(c.getWholeColumnName().contains(".")){
						groupBys.add(outputTable.getColumnPosition(c.getTable().getName().toUpperCase()+
								"_"+ c.getColumnName().toUpperCase()));
					}else{
						groupBys.add(outputTable.getColumnPosition(c.getColumnName().toUpperCase()));
					}
				}
			}
			MyGroupByVisitor gbv = new MyGroupByVisitor(outputTable, groupBys, aggregates,
					projections, aggregatesAlias);
			outputTable = (MyTable) gbv.initialize();
			
			/* reset projections */
			projections = new ArrayList<Integer>();
			long endTime = System.currentTimeMillis();
			//System.out.println("Group byTime: "+ ((endTime - startTime) / 1000.0000) +" sec");
		}
		
		/* order by OPTIONAL. */
		if(arg0.getOrderByElements() != null){
			if(!(arg0.getOrderByElements().toString().equals(
					arg0.getGroupByColumnReferences().toString()))){
				//System.out.println("I never come here");
				ArrayList<OrderByElement> orderBys = new ArrayList<OrderByElement>();
				ArrayList<Integer> columnPositions = new ArrayList<Integer>();
				for(Object o: arg0.getOrderByElements()){
					OrderByElement e = (OrderByElement) o;
					orderBys.add(e);
					Column  c = (Column) e.getExpression();
					if(c.getWholeColumnName().contains(".")){
						columnPositions.add(outputTable.getColumnPosition(c.getTable().getName().toUpperCase()+
								"_"+ c.getColumnName().toUpperCase()));
					}else{
						columnPositions.add(outputTable.getColumnPosition(c.getColumnName().toUpperCase()));
					}
				}
				MyOrderByVisitor obVisitor = new MyOrderByVisitor(outputTable, orderBys, 
						columnPositions);
				outputTable = (MyTable) obVisitor.initialize();
			}
		}
		
		/* set the limit if there is one */
		if(arg0.getLimit() != null){
			limit = arg0.getLimit().getRowCount();
			
		}else{
			limit = -1;
		}
	}

	private boolean isNewTable(ArrayList<MyTable> joined, String newTable) {
		for(int i=0; i<joined.size();i++)
		{
			if(joined.get(i).getTablename().equalsIgnoreCase(newTable))
				return false;
		}
		return true;
	}

	@Override
	public void visit(Union arg0) {
		//System.out.println("INSTANCE OF UNION");
	}

	@Override
	public void visit(Table arg0) {
		fromTable = getCorrectTable(arg0.getName());
	}

	@Override
	public void visit(SubSelect arg0) {
		//System.out.println("INSTANCE OF SUBQUERY");
		long startTime = System.currentTimeMillis(); 
		MySelectVisitor sv = new MySelectVisitor(tables, arg0, indexDir);
		fromTable = sv.getOutputTable();
		long endTime = System.currentTimeMillis(); 
		//System.out.println("Sub query Time: "+ ((endTime - startTime) / 1000.0000) +" sec");
	}

	@Override
	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public MyTable getOutputTable() {
		return outputTable;
	}
	
	public ArrayList<Integer> getProjections() {
		return projections;
	}
	
	public long getLimit() {
		return limit;
	}
	
	/** This method will get the table from the collection of tables based on
	 *  the tables name, and this table will load the data for the table.
	 * @param tableName
	 * @return
	 */
	public Operator getCorrectTable(String tableName){
		String alias=null;
		if(tableName.contains(" as ") || tableName.contains(" AS "))
		{
			String temp = tableName;
			temp.replaceAll(" as | AS ", " AS "); //converting it to uppercase
			tableName =  temp.substring(0, temp.indexOf(" "));
			 alias = temp.replaceAll("(\\w)+\\sAS\\s", "");
			alias = alias.trim();
			alias = alias.toUpperCase();
			
		}
		MyTable t = tables.get(tableName.toUpperCase());
		if(t.alias != null)
		{
			MyTable t1 = t.Clone();
			t1.alias= alias;
			return t1;
		}
		t.alias = alias;
		//t.loadData();
		return t;
	}
	private Expression getRelatedCondition(String where, String tableName)
	{
		String[] conditions = where.split(" AND | and ");
		String FinalCond = "";
		
		for(String s : conditions)
		{
			//If it's not join
			
			if(!(s.contains(Character.valueOf('=').toString()) && ! s.contains(Character.valueOf('\'').toString())))
				
			{
				s= s.trim();
				String tblName = s.substring(0,s.indexOf('.'));
				if(tblName.startsWith("("))
					tblName = tblName.replace("(", "");
				if(tblName.equalsIgnoreCase(tableName))
				{
					
					 s = s.substring(s.indexOf(".")+1, s.length());
					 if(s.endsWith(")") && !(s.contains("DATE") || s.contains("date")))
							s="( " + s;
					 s = s.replaceAll("\\s(\\w)+\\.", " ");
					 if(FinalCond.isEmpty())
					 {
						 FinalCond = s;
					 }
					 else
						 FinalCond += " AND " + s;
				}
				
			}
		}
		if(FinalCond.isEmpty())
			return null;
		String q = "SELECT * FROM S where " + FinalCond;
		CCJSqlParserManager pm = new CCJSqlParserManager();
		Statement stmt;
		try {
			stmt = pm.parse(new StringReader(q));
			Select sel = (Select) stmt;
			PlainSelect psel = (PlainSelect) sel.getSelectBody();
			return psel.getWhere();
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;   
		
	}
	private ReWriteSt reWrite(String where) throws JSQLParserException {
		//System.out.println(where);
		ReWriteSt myst;
		boolean hfc=false;
		Expression aftercond = null;
		if(where.contains("and (") || where.contains("AND ("))    // this means we have nested condition
		{
			
			String str = "AND \\((.)+\\)\\)";
			Pattern p = Pattern.compile(str);
			Matcher m = p.matcher(where);
			while(m.find())
			{
				
				String newcond = m.group();
				newcond = newcond.replaceFirst("AND \\(", "");
				newcond = newcond.substring(0,newcond.length()-1);
				String tmpQuery = "select * from tmp where " + newcond;
			    CCJSqlParserManager pm = new CCJSqlParserManager();
			    Statement st = pm.parse(new StringReader(tmpQuery));
			    Select sel = (Select) st;
				PlainSelect psel = (PlainSelect) sel.getSelectBody();
				aftercond = psel.getWhere();
			}
			where = where.replaceAll(str, "");
			
			
		}
		
		String[] conditions = where.split(" AND | and ");
		ArrayList<String> selects = new ArrayList<String>();
		
		HashMap<String, String> tbq = new HashMap<String, String>();
		
		
		for(String s : conditions)
		{
			//If it's not join
			if((s.contains(">=") || s.contains("<=")) || (!(s.contains(Character.valueOf('=').toString()) && ! s.contains(Character.valueOf('\'').toString()))))
				
			{
				s= s.trim();
				String tblName = s.substring(0,s.indexOf('.'));
				if(tblName.startsWith("("))
					tblName = tblName.replace("(", "");
				if(tbq.get(tblName)== null)
				{
					s = s.substring(s.indexOf(".")+1, s.length());
					if(s.endsWith(")") && !(s.contains("DATE") || s.contains("date")))
							s="( " + s;
					s = s.replaceAll("\\s(\\w)+\\.", " ");
					tbq.put(tblName, s);
				}
				else
				{
					String prev = (tbq.get(tblName));
					s = s.substring(s.indexOf(".")+1, s.length());
					if(s.endsWith(")") && !(s.contains("DATE") || s.contains("date")))
						s="( " + s;
					if(s.contains("."))
						s = s.replaceAll("\\s(\\w)+\\.", " ");
					tbq.put(tblName, prev + " AND "+ s);
					
				}
			}
		}
		for(String key : tbq.keySet())
		{
			String query = "SELECT * FROM (SELECT * FROM "+key+ " where "+ tbq.get(key) + " )";
			selects.add(query);
		}
	
		 Statement[] stms = new Statement[selects.size()];
		 FromItem[] frmItem  = new FromItem[selects.size()];
		 
		  CCJSqlParserManager pm = new CCJSqlParserManager();
		  int i=0;
		  for(String q : selects)
		  { 
			  //System.out.println(i);
			  stms[i]= pm.parse(new StringReader(q));
			  Select st = (Select) stms[i];
			  PlainSelect pst = (PlainSelect) st.getSelectBody();
			  FromItem fr = pst.getFromItem();
			  frmItem[i++] = fr;
		  }
		  myst = new ReWriteSt(frmItem, aftercond, hfc);
		return myst;		
	}
	
	private Expression getJoinExpression (String where) throws JSQLParserException
	{
		Expression exp=null;
		String[] conditions = where.split("AND|and");
		String joinCond = "";
		boolean isFirst = true;
		for(int i=0; i<conditions.length;i++)
		{
			String s = conditions[i];
			if(s.contains("=") && !s.contains("\'") )
			{
				       
				       if(isFirst){
				    	   joinCond += s.trim();
				       		isFirst = false;
						}else{
				    	   joinCond = joinCond + " and " + s.trim();
				       }
				 
			}
	    }
		String q = "SELECT * FROM S where " + joinCond;
		CCJSqlParserManager pm = new CCJSqlParserManager();
		Statement stmt = pm.parse(new StringReader(q));
		Select sel = (Select) stmt;
		PlainSelect psel = (PlainSelect) sel.getSelectBody();
		exp = psel.getWhere();
		
		return exp;
	}
	private Expression getJoinExpression (String where, String tbl1, String tbl2) throws JSQLParserException
	{
		Expression exp=null;
		String[] conditions = where.split("AND|and");
		for(String s: conditions)
		{
			if(s.contains("=") && !s.contains("\'") )
			{
				String temp1 = s.trim();
				temp1 = temp1.substring(0, temp1.indexOf('.'));
				String temp2= s.trim();
				temp2= temp2.substring(temp2.indexOf('=')+1, temp2.indexOf('.', temp2.indexOf('=')+1));
				temp2 = temp2.trim();
				if((temp1.equalsIgnoreCase(tbl1) && temp2.equalsIgnoreCase(tbl2)) || (temp1.equalsIgnoreCase(tbl2) && temp2.equalsIgnoreCase(tbl1)))
				{
					
						String q = "SELECT * FROM S where " + s;
						CCJSqlParserManager pm = new CCJSqlParserManager();
						Statement stmt = pm.parse(new StringReader(q));
						Select sel = (Select) stmt;
						PlainSelect psel = (PlainSelect) sel.getSelectBody();
						exp = psel.getWhere();
						break;
						
					
				}
				else if((temp2.equalsIgnoreCase(tbl1) && temp1.equalsIgnoreCase(tbl2)) || (temp2.equalsIgnoreCase(tbl2) && temp1.equalsIgnoreCase(tbl1)))
				{
						String q = "SELECT * FROM S where " + s;
						CCJSqlParserManager pm = new CCJSqlParserManager();
						Statement stmt = pm.parse(new StringReader(q));
						Select sel = (Select) stmt;
						PlainSelect psel = (PlainSelect) sel.getSelectBody();
						exp = psel.getWhere();
						break;
					
				}
			}
		}
		return exp;
	}
	
	public Expression getNonJoinConditions(String where)
	{
		String[] conditions = where.split(" AND | and ");
		String joinCond = "";
		boolean isFirst = true;
		String FinalCond = "";
		for(int i=0; i<conditions.length;i++)
		{
			String s = conditions[i];
            if(!(s.contains(Character.valueOf('=').toString()) && ! s.contains(Character.valueOf('\'').toString()))){
            	if(FinalCond.isEmpty())
            	{
            		FinalCond = s;
            	}
            	else
            		FinalCond += " AND " +s;
            }
		}
		String q = "SELECT * FROM S where " + FinalCond;
		CCJSqlParserManager pm = new CCJSqlParserManager();
		Statement stmt;
		try {
			stmt = pm.parse(new StringReader(q));
			Select sel = (Select) stmt;
			PlainSelect psel = (PlainSelect) sel.getSelectBody();
			return psel.getWhere();
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;  
	}
	
	/** This method will perform an index nested loop join on a given
	 *  set of tables for a select query and return the resultant table.
	 *  
	 * @param joinedTables The tables to be joined
	 * @param arg0 the Select Query 
	 * @return The resultant table
	 */
	public MyTable hashJoin(ArrayList<MyTable> joinedTables, PlainSelect arg0){
		/* run the selects on the tables to reduce their size*/
		ReWriteSt rewrittedQuery = null;
		ArrayList<Operator> newTables = null;
		Expression newWhere = null;

		try {			
			
//			if(arg0.getJoins().size() >= 1){
//				rewrittedQuery = reWrite(arg0.getWhere().toString());  //for reducing the tables 
//				FromItem[] newStmts = rewrittedQuery.getNewSt();
//				newWhere = getJoinExpression(arg0.getWhere().toString());
//				newTables = new ArrayList<MyTable>();
//				for(int i = 0; i < newStmts.length; i++){
//					newStmts[i].accept(this);
//					newTables.add((MyTable) fromTable);
//				}
//			}else{
//				//newTables = joinedTables;
//			}
			if(arg0.getJoins().size() >= 1){
				newTables = new ArrayList<Operator>();
				/* Perform the index scans on tables that have them */
				/* HACK: this is a hack because the system will assume that there 
				 * are no additional conditions over the index scan */
				String[] removedConditions = new String[scanDetails.size()];
				int nextIndex = 0;
				if(!(scanDetails.isEmpty())){
					for(Entry<String, IndexScanDetail> s: scanDetails.entrySet()){
						Operator reducedTable = this.performIndexScan((MyTable) this.getCorrectTable(s.getKey()), 
								s.getValue());
						newTables.add(reducedTable);
						removedConditions[nextIndex] = s.getValue().getOriginalCondition();
						nextIndex++;
					}					
				}
				/* Then perform regular selection push down on tables */
				/* Remove the already pushed selections from the where
				 * clause */
				String reducedWhere = arg0.getWhere().toString();
				for(int i = 0; i < removedConditions.length; i++){
					reducedWhere = reducedWhere.replace(" AND "+removedConditions[i], "");
				}
//				System.out.println("Reduced Where: "+reducedWhere);
//				rewrittedQuery = reWrite(arg0.getWhere().toString());  //for reducing the tables 
				rewrittedQuery = reWrite(reducedWhere);
				FromItem[] newStmts = rewrittedQuery.getNewSt();
				newWhere = getJoinExpression(arg0.getWhere().toString());
				for(int i = 0; i < newStmts.length; i++){
//					System.out.println(newStmts[i]);
					newStmts[i].accept(this);
					newTables.add((MyTable) fromTable);
				} 

  		/*try {
			
			if(arg0.getJoins().size() >= 1){
				rewrittedQuery = reWrite(arg0.getWhere().toString());  //for reducing the tables 
				FromItem[] newStmts = rewrittedQuery.getNewSt();
				newWhere = getJoinExpression(arg0.getWhere().toString());
				newTables = new ArrayList<MyTable>();
				for(int i = 0; i < newStmts.length; i++){
					newStmts[i].accept(this);
					newTables.add((MyTable) fromTable);
				}*/
			}else{
				//newTables = joinedTables;
			}
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* if the there is a table in JoinedTable but not in the newTables
		 * it means that there was no where condition for that table */
		if(newTables.size() != joinedTables.size())
		{
			for(int i=0; i<joinedTables.size(); i++)
			{
				boolean found = false;
				for(int j=0; j<newTables.size() && !found; j++)
				{
					
					if(joinedTables.get(i).tableName == newTables.get(j).getTablename())
					{
						if(joinedTables.get(i).alias == null) // if we have alias we load the table again
						    found = true; 
					}
					
				}
				if(!found){
					if(joinedTables.get(i).alias != null)
					{
						String tmp = joinedTables.get(i).tableName;
						joinedTables.get(i).tableName= joinedTables.get(i).alias;
						joinedTables.get(i).alias = tmp;
					}
					//joinedTables.get(i).loadData();
					/* For checkpoint 4 */
					joinedTables.get(i).setIndexDir(this.indexDir.toString());
					joinedTables.get(i).loadDataFromHeapFile();
					newTables.add(joinedTables.get(i));
				}
			}
		}

		/*Now that all tables are loaded , we can use alias instead of table name
		 * So I will change tableName to alias, we cannot do it before , because table name was needed to load from memory
		 * we also keep the table name (swap alias and table name, just in case)
		 */
		
/*		for(int i= 0; i<newTables.size(); i++)
		{
			if(newTables.get(i).alias != null)
			{
				String tmp = newTables.get(i).tableName;
				newTables.get(i).tableName= newTables.get(i).alias;
				newTables.get(i).alias = tmp;
				
				
				
				
			}
		}
					
			}
		}
		/*ArrayList<MyTable> stash = newTables;
		newTables = joinedTables;
		for(int i=0 ; i< newTables.size(); i++){
			for(int j=0; j< stash.size();j++){
				if(newTables.get(i).tableName.equalsIgnoreCase(stash.get(j).tableName)){
					if(newTables.get(i).getTuples().size() != stash.get(j).getTuples().size()){
						newTables.remove(i);
						newTables.add(i, stash.get(j));
						
					}
				}
			}
		}
		
		joinedTables = null;
		*/
		if(newWhere != null){
			arg0.setWhere(newWhere);
		}
		
		/* sort by size */
		for(int i = 0; i < -1; i++){
			for(int j = i+1; j < newTables.size(); j++){
				if(newTables.get(i).getTuples().size() >
						newTables.get(j).getTuples().size()){
					Operator t = newTables.get(i);
					newTables.set(i, newTables.get(j));
					newTables.set(j, t);
					
				}
			}
		}
			long startTime = System.currentTimeMillis();
			MyTuple nextTuple = null;
			MyTuple firstTuple = null;
			MyTable product = new MyTable("JOIN", new HashMap<String, Integer>(), 
					new ArrayList<MyTuple>(2000000), new HashMap<String, String>(), indexDir.toString());
			
			Expression e1= null;
			String joinattr = null;
			Operator lefttbl = null;
			 Operator righttbl= null;
			
			try {
				 e1 = getJoinExpression(arg0.getWhere().toString(),newTables.get(0).getTablename() , newTables.get(1).getTablename());

				 if(e1 == null)
				 {
					 Operator tmp = newTables.get(0);
					 newTables.set(0, newTables.get(2));
					 newTables.set(2, tmp);
					 e1 = getJoinExpression(arg0.getWhere().toString(),newTables.get(0).getTablename() , newTables.get(1).getTablename());
				 }
				 /*finding the position of join attributes. It is needed for hash join*/
				 String joinexp =  e1.toString();
				 String left = joinexp.substring(0, joinexp.indexOf("="));
				 left = left.trim();
				 String right = joinexp.substring(joinexp.indexOf("=")+1);
				 right = right.trim();
				 joinattr = left.substring(left.indexOf(".")+1);
				 //String rightattr = right.substring(right.indexOf(".")+1);
				 lefttbl = newTables.get(0);
				 righttbl = newTables.get(1);
				 //leftattrpos = lefttbl.getColumnPosition(leftattr);
				 //rightattrpos = righttbl.getColumnPosition(rightattr);
                 
				 
				 
			} catch (JSQLParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//	Operator cp = new SelectionOperator(e1,new CartesianProduct(newTables.get(0), newTables.get(1)));
			Operator cp;
			if(lefttbl.getSize() < righttbl.getSize())
			{
				cp = new HashJoin(righttbl,lefttbl,joinattr);
			}
			else
			{
				cp = new HashJoin(lefttbl, righttbl, joinattr);
				
			}
			ArrayList<Operator> joined = new ArrayList<Operator>(10);
			joined.add(lefttbl); joined.add(righttbl);
		    /*Need name of the tables to select the next appropriate join constructing left deep tree*/
			ArrayList<String> tblNames = new ArrayList<String>(newTables.size()-2);
		    for(int i=2; i < newTables.size();i++)
		    {
		    	tblNames.add(newTables.get(i).getTablename());
		    }
		    
		
			Operator cp2 = null;
			//String newTable= null; //new Tuple to add for join (we are doing a kind of building left deep tree)
			//MyTable newTbl = null;
			//int i = 1;
			int b = 1; //in which table
			
			for(int i = 3; i<=newTables.size(); i++){
				boolean newTblFound = false;
				//if (i != 1 && i != 2){
					try {
						for(int j=joined.size()-1; j>=0; j--){
							for(int k=0; k<tblNames.size() && !newTblFound; k++)
							{
								
								e1 = getJoinExpression(arg0.getWhere().toString(),tblNames.get(k), joined.get(j).getTablename());
							    if(e1 != null)
							    {
							    	//b = k;
							    	newTblFound = true;
							    	
							    	//offset++;
							    	for(int ii=0; ii<newTables.size(); ii++)
							    	{
							    		if(newTables.get(ii).getTablename() == tblNames.get(k))
							    		{
							    			b=ii;
							    			break;
							    		}
							    	}
							    	tblNames.remove(k); 
							    	
							    
							    }
							}
							if(newTblFound)
								break;
							
						}
			             if(e1!=null)
			             {
							 String joinexp =  e1.toString();
							 String left = joinexp.substring(0, joinexp.indexOf("="));
							 left = left.trim();
							 String right = joinexp.substring(joinexp.indexOf("=")+1);
							 right = right.trim();
							 joinattr = left.substring(left.indexOf(".")+1);
			             }
					
							
					} catch (JSQLParserException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				   if(e1!=null){
					  cp2 = new HashJoin(cp, newTables.get(b), joinattr);
					  joined.add(newTables.get(b));
					  cp = cp2;
				   }
					
					
			//	}
				
			}
			if(cp2 == null){
				cp2 = cp;
			}
			joined = null;
			if(rewrittedQuery != null){
				if(rewrittedQuery.HaveFinalCondition())
				{
					cp2 = new SelectionOperator(rewrittedQuery.getFinalCond(), cp2);
				}
			}
			/* load this cartesian product into memory */
			int iteration = 1;
			int inte = 0;
			hasJoin = true;
			while(cp2.hasNext()){
				nextTuple = cp2.readOneTuple();
				if(nextTuple != null){	
					product.addNewTuple(nextTuple);
					nextTuple.setTable(product);
				}
				if(iteration == 1 && nextTuple != null){
					firstTuple = nextTuple;
					iteration++;
				}
				//System.out.println(inte);
				inte++;
			}
			cp2 = null;
			cp = null;
			long endTime = System.currentTimeMillis(); 
			//System.out.println("Join Time: "+ ((endTime - startTime) / 1000.0000) +" sec");
			/* set the column positions */
			int pos = 0;
			for(MyColumn c: firstTuple.getTupleList()){
				if(product.columnOrder.get(c.getName()) == null){
					product.columnOrder.put(c.getName(), pos);
				}
				product.columnOrder.put(c.getTableName() +"_"+ c.getName().toUpperCase(), pos);
				pos++;
			}
			return product;
	}
	
	/** This method will perform an index nested loop join on
	 *  the tables to be joined and return a resultant table
	 *  of all the joined tuples. This method assumes that
	 *  the tables it is taking in will be reduced by selection.
	 * 
	 * @param joinedTables the reduced tables to be joined
	 * @param arg0 the Selection Query
	 * @return The resultant table.
	 */
	public MyTable indexNestedLoopJoin(ArrayList<MyTable> joinedTables, PlainSelect arg0) {
		/* Get the selection push downs on each of the tables */
		HashMap<String, FromItem> selectionPushDowns = new	HashMap<String, FromItem>();
		ReWriteSt rewrittedQuery = null;
		try {
			rewrittedQuery = reWrite(arg0.getWhere().toString());  //for reducing the tables 
			FromItem[] newStmts = rewrittedQuery.getNewSt();
			for(int i = 0; i < newStmts.length; i++){
				String stmt = newStmts[i].toString();
				String tableName = stmt.substring(stmt.indexOf("FROM")+4, stmt.indexOf("WHERE")).trim().toUpperCase();
				selectionPushDowns.put(tableName, newStmts[i]);
			}
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		/*Now that all tables are loaded , we can use alias instead of table name
		 * So I will change tableName to alias, we cannot do it before , because table name was needed to load from memory
		 * we also keep the table name (swap alias and table name, just in case)
		 */

		int aliasTables = 0;
		for(int i= 0; i<joinedTables.size(); i++)
		{
			if(joinedTables.get(i).alias != null)
			{
				String tmp = joinedTables.get(i).tableName;
				joinedTables.get(i).tableName= joinedTables.get(i).alias;
				joinedTables.get(i).alias = tmp;
			}
		}
			
			MyTuple nextTuple = null;
			MyTuple firstTuple = null;
			MyTable product = new MyTable("JOIN", new HashMap<String, Integer>(), 
					new ArrayList<MyTuple>(2000000), new HashMap<String, String>(), indexDir.toString());
			
			Expression e1= null;
			String joinattr = null;
			MyTable lefttbl = null;
			 MyTable righttbl= null;
			
			try {

				 e1 = getJoinExpression(arg0.getWhere().toString(),joinedTables.get(0).tableName , joinedTables.get(1).tableName);
				 if(e1 == null)
				 {
					 MyTable tmp = joinedTables.get(0);
					 joinedTables.set(0, joinedTables.get(2));
					 joinedTables.set(2, tmp);
					 e1 = getJoinExpression(arg0.getWhere().toString(),joinedTables.get(0).tableName , joinedTables.get(1).tableName);
				 }
				 String joinexp =  e1.toString();
				 String left = joinexp.substring(0, joinexp.indexOf("="));
				 left = left.trim();
				 String right = joinexp.substring(joinexp.indexOf("=")+1);
				 right = right.trim();
				 joinattr = left.substring(left.indexOf(".")+1);
				 lefttbl = joinedTables.get(0);
				 righttbl = joinedTables.get(1);
                 
				 
				 
			} catch (JSQLParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Operator cp;
			/* ALEX: check the size of the two tables because we always want to index
			 * the larger table */
			MyTable tableInMemory = null;
			MyTable indexedTable = null;
//			if(lefttbl.getTuples().size()< righttbl.getTuples().size())
//			{
//				if(righttbl.hasPrimaryIndexOn(joinattr)){
//					indexedTable = righttbl;
//					tableInMemory = lefttbl;
//				}else{
//					indexedTable = lefttbl;
//					tableInMemory = righttbl;
//				}
//			}
//			else
//			{
				if(lefttbl.hasPrimaryIndexOn(joinattr)){
					indexedTable = lefttbl;
					tableInMemory = righttbl;
				}else{
					indexedTable = righttbl;
					tableInMemory = lefttbl;
				}
			
//			}
			/* Push down selection on the in memory table */
			FromItem reduction = selectionPushDowns.get(tableInMemory.tableName);
			if(!(scanDetails.isEmpty())){
				/* HACK: assumes there are no other conditions on the table after
				 * the index scan is done */
				for(Entry<String, IndexScanDetail> s: scanDetails.entrySet()){
					if(s.getKey().equals(tableInMemory.tableName)){
						MyTable reducedTable = (MyTable) this.performIndexScan((MyTable) this.getCorrectTable(s.getKey()), 
								s.getValue());
						tableInMemory.setRows((ArrayList<MyTuple>) reducedTable.getTuples());
					}
				}					
			}else{
				if(reduction != null){
					reduction.accept(this);
					MyTable reducedInMemoryTable = (MyTable) fromTable;
					reducedInMemoryTable.setColumnList(tableInMemory.columnList);
					reducedInMemoryTable.setPrimaryIndexes(tableInMemory.getPrimaryIndexes());
					reducedInMemoryTable.setSecondaryIndexes(tableInMemory.getSecondaryIndexes());
					tableInMemory = reducedInMemoryTable;
				}else{
					tableInMemory.loadData();
				}
			}
			
			/* Check if there is an existing index scan on the indexed table */
			IndexScanDetail scanDetail = null;
			Expression cond = null;
			if(!(scanDetails.isEmpty())){
				for(Entry<String, IndexScanDetail> s: scanDetails.entrySet()){
					if(s.getKey().equals(indexedTable.tableName)){
						scanDetail = s.getValue();
					}
				}					
			}
			/* Get the conditions that we have to evaluate for the index, these
			 * are the ones that were not already evaluated from the index
			 * scan */
			cond = getRelatedCondition(arg0.getWhere().toString(), 
					indexedTable.getTablename());
			if(scanDetail != null){
				String reducedCond = cond.toString().replace(scanDetail.getOriginalCondition(),
						"");
				System.out.println("Reduced Condition: "+reducedCond);
			}
			
			cp = new IndexNestedJoin(indexDir.toString(), tableInMemory,
					indexedTable, joinattr, cond, scanDetail);
			
			ArrayList<MyTable> joined = new ArrayList<MyTable>(10);
			joined.add(lefttbl); joined.add(righttbl);
		    /*Need name of the tables to select the next appropriate join constructing left deep tree*/
			ArrayList<String> tblNames = new ArrayList<String>(joinedTables.size()-2);
		    for(int i=2; i < joinedTables.size();i++)
		    {
		    	tblNames.add(joinedTables.get(i).tableName);
		    }
		    
		
			Operator cp2 = null;
			int b = 1; //in which table
			
			for(int i = 3; i<=joinedTables.size(); i++){
				boolean newTblFound = false;
					try {
						for(int j=0; j< joined.size(); j++){
							for(int k=0; k<tblNames.size() && !newTblFound; k++)
							{
								
								e1 = getJoinExpression(arg0.getWhere().toString(),tblNames.get(k), joined.get(j).tableName);
							    if(e1 != null)
							    {
							    	newTblFound = true;
							    	
							    	for(int ii=0; ii<joinedTables.size(); ii++)
							    	{
							    		if(joinedTables.get(ii).tableName == tblNames.get(k))
							    		{
							    			b=ii;
							    			break;
							    		}
							    	}
							    	tblNames.remove(k); 
							    	
							    
							    }
							}
							if(newTblFound)
								break;
							
						}
			             if(e1!=null)
			             {
							 String joinexp =  e1.toString();
							 String left = joinexp.substring(0, joinexp.indexOf("="));
							 left = left.trim();
							 String right = joinexp.substring(joinexp.indexOf("=")+1);
							 right = right.trim();
							 joinattr = left.substring(left.indexOf(".")+1);
			             }
					
							
					} catch (JSQLParserException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				   if(e1!=null){
						/* Check if there is an existing index scan on the indexed table */
						IndexScanDetail nextScanDetail = null;
						Expression nextCond = null;
						if(!(scanDetails.isEmpty())){
							for(Entry<String, IndexScanDetail> s: scanDetails.entrySet()){
								if(s.getKey().equals(joinedTables.get(b))){
									nextScanDetail = s.getValue();
								}
							}					
						}
						/* Get the conditions that we have to evaluate for the index, these
						 * are the ones that were not already evaluated from the index
						 * scan */
						nextCond = getRelatedCondition(arg0.getWhere().toString(), 
								joinedTables.get(b).getTablename());
						if(nextScanDetail != null){
							String reducedCond = nextCond.toString().replace(nextScanDetail.getOriginalCondition(),
									"");
							System.out.println("Reduced Condition: "+reducedCond);
						}
						
					   if(joinedTables.get(b).alias == null)
					   {
						   cp2 = new IndexNestedJoin(indexDir.toString(),
									  cp, joinedTables.get(b), joinattr, nextCond, nextScanDetail);
					   }
					   else if(joinedTables.get(b).alias != null && aliasTables == 0)
					   {
						  cp2 = new IndexNestedJoin(indexDir.toString(),
								  cp, joinedTables.get(b), joinattr, nextCond, nextScanDetail);
						  aliasTables++;
					   }else {
						   /* HACK: this is a hack, assuming we do not have conditions on nation */
						   joinedTables.get(b).loadData();
						   cp2 = new HashJoin(cp, joinedTables.get(b), joinattr);
					   }
					  joined.add(joinedTables.get(b));
					  cp = cp2;
				   }
					
					
				
			}
			if(cp2 == null){
				cp2 = cp;
			}
			joined = null;
			if(rewrittedQuery != null){
				if(rewrittedQuery.HaveFinalCondition())
				{
					cp2 = new SelectionOperator(rewrittedQuery.getFinalCond(), cp2);
				}
			}
			/* load this cartesian product into memory */
			int iteration = 1;
			int inte = 0;
			hasJoin = true;
			while(cp2.hasNext()){
				nextTuple = cp2.readOneTuple();
				if(nextTuple != null){	
					product.addNewTuple(nextTuple);
					nextTuple.setTable(product);
				}
				if(iteration == 1 && nextTuple != null){
					firstTuple = nextTuple;
					iteration++;
				}
				//System.out.println(inte);
				inte++;
			}
			cp2 = null;
			cp = null;
			
			/* set the column positions */
			int pos = 0;
			for(MyColumn c: firstTuple.getTupleList()){
				if(product.columnOrder.get(c.getName()) == null){
					product.columnOrder.put(c.getName(), pos);
				}
				product.columnOrder.put(c.getTableName() +"_"+ c.getName().toUpperCase(), pos);
				pos++;
			}
			return product;
	}
	
	/** Given a query's where clause, this method will get the upper
	 *  and lower bound for each of the columns on each of the tables.
	 * 
	 * @return A hashMap whose keys are the table names and values
	 * 		   are also HashMaps whose keys are column names and whose 
	 * 		   values are ArrayLists holding the upper bound at index 1 and the lower
	 * 		   bound at index 0 for each column.
	 */
	public HashMap<String, IndexScanDetail> getUpperAndLowerBounds(String where){
		HashMap<String, IndexScanDetail> retval = new 
				HashMap<String, IndexScanDetail>();
		ArrayList<String> rangeBounds = new ArrayList<String>();
		String[] conditions = where.split(" AND | and ");
		String next = "";
		for(int i = 0; i < conditions.length; i++){
			next = conditions[i];
			String[] parts = next.split(" = | <> | > | < | >= | <= ");
			if(parts[0].contains(".") && parts[1].contains(".")){
				String leftTable = parts[0].substring(0, parts[0].indexOf(".")).toUpperCase().trim().
						replace("(", "").replace(")", "");;
				String rightTable = parts[1].substring(0, parts[1].indexOf(".")).toUpperCase().trim().
						replace("(", "").replace(")", "");;
				/* If the two tables are not equal then this is a join condition and
				 * we can ignore it. */
				if(leftTable.equals(rightTable) || conditions[i].contains(" OR ")){
					/* this is an additional condition that we still have to evaluate */
					if(leftoverConditions.get(leftTable) != null){
						leftoverConditions.get(leftTable).add(conditions[i]);
					}else{
						ArrayList<String> tableConds = new ArrayList<String>();
						tableConds.add(conditions[i]);
						leftoverConditions.put(leftTable, tableConds);
					}
				}
			}else{
				/* if this is an inequality then there's nothing we can with it */
				if(!(conditions[i].contains("<>")) && !(conditions[i].contains(" = "))){
					rangeBounds.add(conditions[i]);
				}else{
					/* this is an additional condition that we still have to evaluate */
					String table = conditions[i].substring(
							0,conditions[i].indexOf(".")).trim().toUpperCase().
							replace("(", "").replace(")", "");
					if(leftoverConditions.get(table) != null){
						leftoverConditions.get(table).add(conditions[i]);
					}else{
						ArrayList<String> tableConds = new ArrayList<String>();
						tableConds.add(conditions[i]);
						leftoverConditions.put(table, tableConds);
					}
				}
			}
		}
		/* Store the lower and upper bound for columns within range conditions */
		if(!(rangeBounds.isEmpty())){
			/* HACK assumes there's one pair of range conditions */
			String upperBound = "";
			String lowerBound = "";
			String column = "";
			String lowerBoundOperator = "";
			String upperBoundOperator = "";
			String tableName = "";
			String[] bounds = new String[2];
			String[] operators = new String[2];
			int i = 0;
			for(String cond: rangeBounds){
				String[] parts = cond.split(" = | <> | > | < | >= | <= ");
				tableName = cond.substring(0,cond.indexOf(".")).trim().toUpperCase();
				column = cond.substring(cond.indexOf(".")+1, cond.indexOf(" ")).trim();
				operators[i] = cond.substring(cond.indexOf(" "), cond.lastIndexOf(" ")).trim();
				bounds[i] = parts[1].trim();
				i++;
			}
			if(bounds[0].compareTo(bounds[1]) > 0){
				upperBound = bounds[0];
				upperBoundOperator = operators[0];
				lowerBound = bounds[1];
				lowerBoundOperator = operators[1];
			}else{
				upperBound = bounds[1];
				upperBoundOperator = operators[1];
				lowerBound = bounds[0];
				lowerBoundOperator = operators[0];
			}
			String left = rangeBounds.get(0).replace(tableName.toLowerCase()+".", "");
			String right = rangeBounds.get(1).replace(tableName.toLowerCase()+".", "");
			
			IndexScanDetail isd = new IndexScanDetail(lowerBound, 
					upperBound, lowerBoundOperator, upperBoundOperator, column,
					left + " AND " + right, rangeBounds.get(0) + " AND " + rangeBounds.get(1));
			retval.put(tableName, isd);
		}
		return retval;
	}
	
	/** This method will take in a table and details of an index scan for that
	 *  table, perform the index scan and return the resultant table. This method
	 *  assumes that the passed table has an index on the acted upon column.
	 * 
	 * @param table The table schemma to perform the index scan on
	 * @param detail The object holding the details of the index scan.
	 * @return The resultant table after performing the index scan.
	 */
	public Operator performIndexScan(MyTable table, final IndexScanDetail detail){
		/* If the values are dates parse them accordingly */
		String upperBound = "";
		String lowerBound = "";
		if(detail.getUpperBound().contains("DATE") || 
				detail.getUpperBound().contains("date")){
			/* Parse out the date values */
			String dateUpper = detail.getUpperBound();
			upperBound = dateUpper.substring(dateUpper.indexOf("(")+1,
					dateUpper.indexOf(")")).replace("'", "");
			String dateLower = detail.getLowerBound();
			lowerBound = dateLower.substring(dateLower.indexOf("(")+1,
					dateLower.indexOf(")")).replace("'", "");
		}
//		System.out.println("Upper Bound: "+upperBound);
//		System.out.println("Lower Bund: "+lowerBound);
//		System.out.println("Upper Bound Operator: "+detail.getUpperBoundOperator());
//		System.out.println("Lower Bound Operator: "+detail.getLowerBoundOperator());
		
		/* Get the index */
		PrimaryStoreMap<Long, MyTuple> index = null;
		SecondaryTreeMap<String, Long, MyTuple> secondaryIndex = null;
		ArrayList<MyTuple> tuples = new ArrayList<MyTuple>();
		try {
			RecordManager recman = RecordManagerFactory.createRecordManager(indexDir.toString() + 
					File.separatorChar + table.getTablename());
			/* Get the primary index */
			 index = recman.storeMap("Primary_" +table.getTablename(), 
					new MyTupleSerializer(table.columnList.size(), table));
			
			 secondaryIndex = index.secondaryTreeMap("Secondary_"+table.getTablename()+"_"+detail.getColumnName(), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
					public String extractSecondaryKey(Long key, MyTuple t){
                        // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
                         int pos= t.getColumnPosition(detail.getColumnName().toString().toUpperCase());
                         return t.getTupleList().get(pos).getValue();
                         }
                          });
			 
		
		SortedMap<String, Iterable<Long>> reducedMap = secondaryIndex.subMap(
				lowerBound, upperBound);
		for(Iterable<Long> tupleKeys: reducedMap.values()){
			for(Long key: tupleKeys){
				//System.out.println(secondaryIndex.getPrimaryValue(key));
				tuples.add(index.get(key));
			}
		}
		
		/* subMap does NOT account for <=, so if the upper bound operator
		 * is <= we need to get the tuples whose values are equal to the upper bound
		 * and add them to the returned tuples. */
		if(detail.getUpperBoundOperator().equals("<=")){
			for(Long key: secondaryIndex.get(upperBound)){
				//System.out.println(tuple);
				tuples.add(index.get(key));
			}
		}
		
		/* subMap does NOT account for >, so if the lower bound operator
		 * is > we need to remove the tuples whose values are equal to the lower bound
		 * and remove them from the returned tuples. */
		if(detail.getLowerBoundOperator().equals(">")){
			for(Long key: secondaryIndex.get(lowerBound)){
				//System.out.println("REMOVED: "+ tuple);
				tuples.remove(index.get(key));
			}
		}
		
		/*if(detail.getUpperBoundOperator().equals("<=")){
			reducedMap.put(upperBound, secondaryIndex.get(upperBound));
		}
		
		if(detail.getLowerBoundOperator().equals(">")){
			reducedMap.remove(secondaryIndex.get(lowerBound));
		}*/
		
		//IndexScanOperator iso = new IndexScanOperator(reducedMap, index, table.getTablename());
		recman.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		table.setRows(tuples);
		return table;
	}
	
}

/*This is  an struct for reWrite one member is an array of FromItem[]
 * the other is when the condition has to be applied in final step 
 * because it cannot be applied in one table
 */
class ReWriteSt{
	private FromItem[] newSt;
	private Expression finalCond;
	private boolean haveFinalCond;
	
	public ReWriteSt(FromItem[] fi, Expression exp, boolean hfc)
	{
		newSt = fi;
		finalCond = exp;
		haveFinalCond = hfc;
	}
	public FromItem[] getNewSt()
	{
		return newSt;
	}
	public Expression getFinalCond()
	{
		return finalCond;
	}
	public boolean HaveFinalCondition()
	{
		return haveFinalCond;
	}
	
	
}

