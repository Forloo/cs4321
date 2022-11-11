package p1.logicaloperator;

import p1.util.Aliases;

/**
 * Logical version of ScanOperator.
 */
public class LogicalScan extends LogicalOperator {

	// The name of the table (aliased) to scan
	private String table;

	/**
	 * The constructor for the logical scan operator
	 *
	 * @param fromTable The name of the table (aliased) to scan
	 */
	public LogicalScan(String fromTable) {
		this.table = fromTable;
	}

	/**
	 * Retrieves the table to scan
	 *
	 * @return The name of the table (alias name if using aliases)
	 */
	public String getFromTable() {
		return table;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical scan node";
	}

	/**
	 * Gets the string to print for the logical plan
	 * 
	 * @param level the level of the operator
	 * @return the logical plan in string form
	 */
	public String toString(int level) {
		return "-".repeat(level) + "Leaf[" + Aliases.getTable(table) + "]\n";
	}
}
