package p1.logicaloperator;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalSort extends LogicalOperator {
	
	// The query containing the information for our logical sort
	private PlainSelect plainSelect;
	// The table that we want to sort on.
	private String tableName;
	
	/**
	 * A relational algebra representation of the sort operator
	 * @param plainSelect A plainselect containing the information 
	 * @param tableName The table that we are sorting on.
	 */
	public LogicalSort(PlainSelect plainSelect,String tableName) {
		this.plainSelect=plainSelect;
		this.tableName=tableName;
	}
	
	/**
	 * Retrieves the plainselect that holds all of our information
	 * @return A plainselect object containing information that we want to sort on
	 */
	public PlainSelect getInfo() {
		return plainSelect;
	}
	
	/**
	 * Retrieves the tableName that we want to sort on.
	 * @return A string representing the name of the table.
	 */
	public String getName() {
		return tableName;
	}
	
	// This is for testing only.
	public String toString() {
		return "This is a logical sort";
	}
	
}
