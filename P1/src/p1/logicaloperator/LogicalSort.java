package p1.logicaloperator;

import java.util.List;

/**
 * Logical version of SortOperator.
 */
public class LogicalSort extends LogicalOperator {

	// The child operator for this should be some logical operator
	private LogicalOperator child;
	// The expression to filter by
	private List orderBy;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param op     the child operator
	 * @param orders the columns to order by
	 */
	public LogicalSort(LogicalOperator op, List orders) {
		this.child = op;
		this.orderBy = orders;
	}

	/**
	 * Retrieves the child operator.
	 *
	 * @return The child operator used to get tuples.
	 */
	public LogicalOperator getChild() {
		return child;
	}

	/**
	 * Retrieves the columns to sort by.
	 *
	 * @return A list containing the columns to order by.
	 */
	public List getOrderBy() {
		return orderBy;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical sort node";
	}

	/**
	 * Gets the string to print for the logical plan
	 * 
	 * @param level the level of the operator
	 * @return the logical plan in string form
	 */
	public String toString(int level) {
		if (orderBy == null) {
			return child.toString(level + 1);
		}
		return "-".repeat(level) + "Sort" + orderBy.toString() + "\n" + child.toString(level + 1);
	}
}
