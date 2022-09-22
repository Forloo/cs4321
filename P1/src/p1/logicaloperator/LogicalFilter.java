package p1.logicaloperator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalFilter extends LogicalOperator {
		
	// The table to check the condition from
	private String tableName;
	// The query containing the information for the logical filter.
	private PlainSelect plainSelect;

	/**
	 * The constructor for the logical filter operator
	 * @param tableName The table we are filtering on
	 * @param plainSelect Query containing the information
	 */
	public LogicalFilter(String tableName, PlainSelect plainSelect) {
		this.tableName=tableName;
		this.plainSelect=plainSelect;
	}
	
	/**
	 * Retrieves the table we are performing filter on
	 * @return The name of the table that we are filtering on
	 */
	public String getTable() {
		return tableName;
	}
	
	/**
	 * Retrieves the query containing the conditions that we are filtering on
	 * @return A plainSelect object containing the condition information.
	 */
	public PlainSelect getInfo() {
		return plainSelect;
	}
}
