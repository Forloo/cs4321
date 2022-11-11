package p1.logicaloperator;

import net.sf.jsqlparser.statement.select.Distinct;

/**
 * Logical version of DuplicateEliminationOperator.
 */
public class LogicalUnique extends LogicalOperator {

	// The child operator
	private LogicalSort child;
	// There is no expression evaluate by
	private Distinct distinct;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param child The child operator; must be sort
	 */
	public LogicalUnique(LogicalSort child) {
		this.child = child;
	}

	/**
	 * Retrieves the child operator.
	 *
	 * @return The child operator used to get tuples.
	 */
	public LogicalSort getChild() {
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

	/**
	 * Gets the string to print for the logical plan
	 * 
	 * @param level the level of the operator
	 * @return the logical plan in string form
	 */
	public String toString(int level) {
		return "-".repeat(level) + "DupElim\n" + child.toString(level + 1);
	}
}
