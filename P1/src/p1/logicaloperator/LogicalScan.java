package p1.logicaloperator;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalScan extends LogicalOperator{

	// The tablename for our table
	private String tableName;
	// The query containing the information
	private PlainSelect plainSelect;
	
	/**
	 * A constructor for the logicalscan
	 * @param plainselect The query containing the information that we need
	 * @param fromTable The table name
	 */
	public LogicalScan(PlainSelect plainselect,String fromTable) {
		this.plainSelect=plainSelect;
		this.tableName=fromTable;
	}
	
	/**
	 * Retrieves the query containing our information
	 * @return A plainselect containing the query information that we need.
	 */
	public PlainSelect getInfo() {
		return plainSelect;
	}
	
	/**
	 * Retrieves the name of the table
	 * @return A string representing the name of the table 
	 */
	public String getName() {
		return tableName;
	}
	
	// Using this for testing purporses only.
	public String toString() {
		return "This is a logical scan";
	}
}
