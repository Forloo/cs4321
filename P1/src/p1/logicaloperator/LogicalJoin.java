package p1.logicaloperator;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalJoin extends LogicalOperator{
	
	// The query containing the information for our join
	private PlainSelect plainSelect;
	// The table that we are performing the join. Null if the tables do not exist.
	private String tableName;
	
	/**
	 * The constructor for our logicalJoin
	 * @param plainSelect: The plainSelect containing the information for our query
	 * @param tableName
	 */
	public LogicalJoin(PlainSelect plainSelect, String tableName) {
		this.plainSelect=plainSelect;
		this.tableName=tableName;
	}
	
	/**
	 * Retrieves the query information
	 * @return A plainSelect
	 */
	public PlainSelect getInfo() {
		return plainSelect;
	}
	
	/**
	 * Retrieves the tableName
	 * @return A string representing the tableName.
	 */
	public String getName() {
		return tableName;
	}
	
	// This is for testing only.
	public String toString() {
		return "This is a join node";
	}
	
}
