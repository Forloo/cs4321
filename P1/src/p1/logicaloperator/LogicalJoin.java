package p1.logicaloperator;

import net.sf.jsqlparser.expression.Expression;

public class LogicalJoin extends LogicalOperator {

	// The left child operator
	private LogicalOperator left;
	// The right child operator
	private LogicalOperator right;
	// The expression to filter by
	private Expression exp;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param left  The left child operator
	 * @param right The right child operator
	 * @param ex    Expression containing the information
	 */
	public LogicalJoin(LogicalOperator left, LogicalOperator right, Expression ex) {
		this.left = left;
		this.right = right;
		this.exp = ex;
	}

	/**
	 * Retrieves the left child operator.
	 *
	 * @return The left child operator used to get tuples.
	 */
	public LogicalOperator getLeftChild() {
		return left;
	}

	/**
	 * Retrieves the left child operator.
	 *
	 * @return The left child operator used to get tuples.
	 */
	public LogicalOperator getRightChild() {
		return right;
	}

	/**
	 * Retrieves the expression containing the conditions that we are filtering on
	 *
	 * @return An Expression object containing the condition information.
	 */
	public Expression getExpression() {
		return exp;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical join node";
	}
}
