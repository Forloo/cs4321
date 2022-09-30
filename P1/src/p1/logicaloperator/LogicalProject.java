package p1.logicaloperator;

import java.util.List;

import p1.operator.Operator;

public class LogicalProject extends LogicalOperator {

	// The child operator
	private Operator child;
	// The columns to select
	private List selects;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param op      The child operator
	 * @param selects The list of columns to select
	 */
	public LogicalProject(Operator op, List selects) {
		this.child = op;
		this.selects = selects;
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
	 * Retrieves the select items for columns to return.
	 *
	 * @return A SelectItem list containing the column information.
	 */
	public List getSelects() {
		return selects;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical project node";
	}
}
