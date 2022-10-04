package p1.logicaloperator;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;

public class LogicalJoin extends LogicalOperator {

	// The left child operator
	private LogicalOperator left;
	// The right child operator
	private LogicalOperator right;
	// The expression to filter by
	private ArrayList<Expression> exp;
	// A string telling us the tables needed for operator
	private String tables;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param left  The left child operator
	 * @param right The right child operator
	 * @param ex    Expression containing the information
	 */
	public LogicalJoin(String tables,LogicalOperator left, LogicalOperator right, ArrayList<Expression> ex) {
		this.tables=tables;
		this.left = left;
		this.right = right;
		this.exp = ex;
	}
	
	/**
	 * Retrieves the tables that this join table contains
	 * @return A string delimited by commas telling us the tables that are joined.
	 */
	public String getTables() {
		return tables;
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
	public ArrayList<Expression> getExpression() {
		return exp;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical join node";
	}
}
