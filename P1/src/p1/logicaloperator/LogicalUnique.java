package p1.logicaloperator;

import net.sf.jsqlparser.statement.select.Distinct;
import p1.operator.Operator;

public class LogicalUnique extends LogicalOperator {

	// The child operator
	private LogicalOperator child;
	// The expression to filter by
	private Distinct distinct;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param op The child operator
	 * @param d  Whether or not we want distinct tuples
	 */
	public LogicalUnique(LogicalOperator op, Distinct d) {
		this.child = op;
		this.distinct = d;
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
	 * Retrieves whether or not we want distinct tuples
	 *
	 * @return A Distinct object that determines if we want distinct tuples.
	 */
	public Distinct getDistinct() {
		return distinct;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical unique node";
	}
}
