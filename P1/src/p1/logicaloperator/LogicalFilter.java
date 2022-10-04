package p1.logicaloperator;

import net.sf.jsqlparser.expression.Expression;

public class LogicalFilter extends LogicalOperator {

	// The child operator
	private LogicalOperator child;
	// The expression to filter by
	private Expression exp;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param op The child operator
	 * @param ex Expression containing the information
	 */
	public LogicalFilter(LogicalOperator op, Expression ex) {
		this.child = op;
		this.exp = ex;
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
	 * Retrieves the expression containing the conditions that we are filtering on
	 *
	 * @return An Expression object containing the condition information.
	 */
	public Expression getExpression() {
		return exp;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical filter node";
	}
}
