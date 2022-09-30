package p1.logicaloperator;

import java.util.List;

import p1.operator.Operator;

public class LogicalSort extends LogicalOperator {

	// The child operator
	private Operator child;
	// The expression to filter by
	private List orderBy;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param op     the child operator
	 * @param orders the columns to order by
	 */
	public LogicalSort(Operator op, List orders) {
		this.child = op;
		this.orderBy = orders;
	}

	/**
	 * Retrieves the child operator.
	 *
	 * @return The child operator used to get tuples.
	 */
	public Operator getChild() {
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
}
