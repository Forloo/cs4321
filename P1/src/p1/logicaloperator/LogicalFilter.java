package p1.logicaloperator;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import p1.unionfind.UnionFindElement;

/**
 * Logical version of SelectOperator.
 */
public class LogicalFilter extends LogicalOperator {

	// The child operator
	private LogicalOperator child;
	// The expression to filter by
	private ArrayList<Expression> exp;
	// The constraints from the unionfind
	private ArrayList<UnionFindElement> ufRestraints;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param op The child operator
	 * @param ex Expression containing the information
	 */
	public LogicalFilter(LogicalOperator op, ArrayList<Expression> expr, ArrayList<UnionFindElement> ufRestraints) {
		this.child = op;
		this.exp = expr;
		this.ufRestraints=ufRestraints;
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
	public ArrayList<Expression> getExpression() {
		return exp;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical filter node";
	}
	
	public ArrayList<UnionFindElement> getUfRestraints(){
		return this.ufRestraints;
	}

	/**
	 * Gets the string to print for the logical plan
	 * 
	 * @param level the level of the operator
	 * @return the logical plan in string form
	 */
	public String toString(int level) {
		System.out.println("Logical Filter CAlled");
		return "-".repeat(level) + "Select[" + exp.toString() + "]\n" + child.toString(level + 1);
	}
}
