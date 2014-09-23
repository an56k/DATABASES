package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;

/* This class computes the index nested loop join */

public class IndexNestedJoin implements Operator {
	
	/** The current working tuple from the table in memory */
	private MyTuple tup1;
	
	/** If we are dealing with a secondary index, this is the current working list of
	 * tuples */
	private Iterator<Long> tupleKeys; 
	
	/** The index object itself */
	private PrimaryStoreMap<Long, MyTuple> index;
	
	/** The secondary index, if we are dealing with one */
	private SecondaryTreeMap<String, Long, MyTuple> secondaryIndex;
	//private SecondaryTreeMap<String, String, MyTuple> secondaryIndex;
	
	/** The in memory table to join */
	private Operator tableInMemory;
	
	/** A boolean to tell whether we are using a primary index or 
	 * secondary index */
	private boolean isPrimary;
	
	/** The record manager for the index */
	private RecordManager recman; 
	
	/** The condition upon the index */
	private Expression indexCondition;
	
	/** The name of the join attribute */
	private String joinAttrName;
	
	/** This class' instance of MyExpressionVisitor */
	private MyExpressionVisitor expVisitor;

	/** If the record manager is closed*/
	private boolean isRecmanClosed = false;
	
	/** An index scan detail object that will flag the existence of an
	 * index scan over the indexed table */
	private IndexScanDetail scanDetail;

    int frstCnTpl = 0;
    MyTable schema = new MyTable(null,null);
	/** A constructor for an index nested loop join.
	 * 
	 * @param indexDir the string path to the index directory
	 * @param joinAttribute the column position of the join attribute of the table in memory
	 * @param tableInMemory the left table that we will read from memory
	 * @param indexedTable the table that we will perform index scans for
	 */
	public IndexNestedJoin(String indexDir, Operator tableInMemory,
			Operator indexedTable, final String joinAttrName, Expression indexCondition,
			IndexScanDetail scanDetail) {
		this.tupleKeys = null;
		this.joinAttrName = joinAttrName;
		this.tableInMemory = tableInMemory;
		this.indexCondition = indexCondition;

		this.scanDetail = scanDetail;

		this.expVisitor = new MyExpressionVisitor(null);
		MyTable castedIndexTable = (MyTable) indexedTable;

		try {
			if(castedIndexTable.alias==null){
				
				this.recman = RecordManagerFactory.createRecordManager(indexDir.toString() + 
						File.separatorChar + castedIndexTable.getTablename());
			}
			else{
					this.recman = RecordManagerFactory.createRecordManager(indexDir.toString() + 
							File.separatorChar + castedIndexTable.alias);
			}
				
			/* Get the primary index */
			if(castedIndexTable.alias!=null)
			{
				
				index = recman.storeMap("Primary_" +castedIndexTable.alias, 
						new MyTupleSerializer(castedIndexTable.columnList.size(), castedIndexTable));
			}else{
				index = recman.storeMap("Primary_" +castedIndexTable.getTablename(), 
						new MyTupleSerializer(castedIndexTable.columnList.size(), castedIndexTable));
			}

			/* check whether or not this is a primary index with a singular key */
			if(castedIndexTable.hasConjoinedKey()){
				this.isPrimary = false;
				/* grab the secondary index on the join attribute */
				if(castedIndexTable.hasSecondaryIndexOn(joinAttrName)){
					//secondaryIndex = recman.treeMap("Secondary_" +castedIndexTable.getTablename()+
						//	"_" +joinAttrName, new KeyArraySerializer())  ;
					secondaryIndex = index.secondaryTreeMap("Secondary_"+castedIndexTable.getTablename()+"_"+joinAttrName, new SecondaryKeyExtractor<String, Long, MyTuple>() {   
						public String extractSecondaryKey(Long key, MyTuple t){
						                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
						                              int pos= t.getColumnPosition(joinAttrName);
						                              return t.getTupleList().get(pos).getValue();
						                              }
					 });
					}
 			}else{
				this.isPrimary = true;
				/* We will use the primary index */
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void resetStream() {
		tableInMemory.resetStream();
	}

	@Override
	public MyTuple readOneTuple() {
		/* If we are dealing with a primary index then there can only be one tuple 
		 * match in the index for every tuple in the table scan */
		MyTuple tup2 = null;
		if(isPrimary){
			while(tup2 == null){
				/* read one tuple from the in memory table */
				tup1 = tableInMemory.readOneTuple();
				
				/* retrieve the tuple that matches the join attribute from the index */
				if(tup1 != null){
					tup2 = index.get(tup1.getColumn(
							tup1.getColumnPosition(joinAttrName)).getValue());
					if(tup2 != null){
						if(indexCondition != null){
							expVisitor.setTuple(tup2);
							indexCondition.accept(expVisitor);
							if(!(expVisitor.getBooleanResult())){
								tup2 = null;
							}
						}
					}

				}else{
					return null;
				}
			}
		/* If we are dealing with a secondary index then there could potentially be
		 * > 1 tuples that will join with each tuple of the table scan. */
		}else{
			while(tup2 == null){
				while(tupleKeys == null){
					/* read one tuple from the in memory table */
					tup1 = tableInMemory.readOneTuple();
					
					/* Retrieve the set of tuple keys that join with tup1 */
					if(tup1 != null){
						
						tupleKeys = (Iterator<Long>) secondaryIndex.get(tup1.getColumn(
								tup1.getColumnPosition(joinAttrName)).getValue());
					}else{
						return null;
					}
				}
				/* Set tup2 to be the next tuple retrieved from the index */
				tup2 = index.get(tupleKeys.next());
				if(indexCondition != null){
					expVisitor.setTuple(tup2);
					indexCondition.accept(expVisitor);
					if(!(expVisitor.getBooleanResult())){
						tup2 = null;
					}
				}
				if(!tupleKeys.hasNext()){
					tupleKeys = null;
				}
			}
		}
		/* return the next joined tuple */
		MyTuple retval = MyTuple.ConcatTuple(tup1, tup2);
		frstCnTpl ++;
		if(frstCnTpl == 1)
		{
			schema = MyTuple.concatSchema(tup1, tup2);
		}
		retval.setTable(schema);
		return retval;
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
	public boolean hasNext() {
		if(tableInMemory.hasNext() || tup1 != null){
			return true;

		}else if(isRecmanClosed){
			return false;
		}else{
			try {
				recman.close();
				isRecmanClosed = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}
	}

	@Override
	public List<MyTuple> getTuples() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTablename() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getSize() {
		return tableInMemory.getSize();
	}

}
