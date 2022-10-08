package p1.logicaloperator;

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
}
