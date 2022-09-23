package p1.logicaloperator;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalProject extends LogicalOperator {
	
	// The query containing the information for the logical project
	private PlainSelect plainSelect;
	// The table that we are performing the project
	private String tableName;
		
	/**
	 * The constructor for the logical project
	 * @param plainselect The plainselect containing the information for our logical project
	 * @param fromTable The table we are performing the project on
	 */
	public LogicalProject(PlainSelect plainselect,String fromTable) {
		this.plainSelect=plainselect;
		this.tableName=fromTable;
			}
	
	/**
	 * Retrieves the query containing the information for logical project
	 * @return A plainselect that contains the information we need for the logical project
	 */
	public PlainSelect getInfo() {
		return plainSelect;
	}
	
	/**
	 * Retrieves the table name.
	 * @return A string representation of the table.
	 */
	public String getName() {
		return tableName;
	}
}
