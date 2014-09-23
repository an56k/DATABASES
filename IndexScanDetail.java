package edu.buffalo.cse562;

/** This simple class will store the index scan details for a
 *  potential index scan such as upper and lower bounds and
 *  equality/inequality operators.
 * 
 * @author Alex
 *
 */
public class IndexScanDetail {
	
	/** The parsed condition (without table names) */
	private String taleSpecificCondition;
	
	/** The actual condition from the where clause */
	private String originalCondition;
	
	/** The name of the column that this detail pertains to */
	private String columnName;
	
	/** The value for the lower bound */
	private String lowerBound;
	
	/** The value for the lower bound */
	private String upperBound;
	
	/** This details lower bound operator */
	private String lbo;
	
	/** This details upper bound operator */
	private String ubo;
	
	public IndexScanDetail(String lowerBound, String upperBound, String lbo,
			String ubo, String columnName, String taleSpecificCondition,
			String originalCondition){
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		this.lbo = lbo;
		this.ubo = ubo;
		this.columnName = columnName;
		this.taleSpecificCondition = taleSpecificCondition;
		this.originalCondition = originalCondition;
	}

	public String getOriginalCondition() {
		return originalCondition;
	}

	public void setOriginalCondition(String originalCondition) {
		this.originalCondition = originalCondition;
	}

	public String getTableSpecificCondition() {
		return taleSpecificCondition;
	}

	public void setTableSpecificCondition(String condition) {
		this.taleSpecificCondition = condition;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getLowerBound() {
		return lowerBound;
	}

	public String getUpperBound() {
		return upperBound;
	}

	public String getLowerBoundOperator() {
		return lbo;
	}

	public String getUpperBoundOperator() {
		return ubo;
	}

	public void setLowerBound(String lowerBound) {
		this.lowerBound = lowerBound;
	}

	public void setUpperBound(String upperBound) {
		this.upperBound = upperBound;
	}

	public void setLbo(String lbo) {
		this.lbo = lbo;
	}

	public void setUbo(String ubo) {
		this.ubo = ubo;
	}

}
