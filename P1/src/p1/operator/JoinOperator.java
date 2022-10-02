package p1.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import p1.io.BinaryTupleWriter;
import p1.util.Aliases;
import p1.util.ExpressionEvaluator;
import p1.util.ExpressionParser;
import p1.util.Tuple;

/**
 * An operator that processes queries on multiple files and conditions.
 */
public class JoinOperator extends Operator {

	// Left child operator
	private Operator left;
	// Right child operator
	private Operator right;
	// Tree representing the join operator
	private JoinOperatorTree tree;
	// A list of tuples containing the final results.
	private ArrayList<Tuple> results;
	// Index for which tuple we are on
	private int idx;
	// The schema for the results query table
	private ArrayList<String> schema;
	// The where condition. Only used to call accept.
	private Expression where;
	// The left tuple to join with.
	private Tuple leftTuple;
	// The tables that are being joined on by this joinoperator
	private String tables;

	/**
	 * Creates a JoinOperatorTree.
	 *
	 * @param left  the left child operator
	 * @param right the right child operator
	 * @param exp The where expression will be passed in by the query plan.
	 */
	public JoinOperator(String tables,Operator left, Operator right, Expression exp) {
		this.left = left;
		this.right = right;
		
		where=exp;
		schema = left.getSchema();
		schema.addAll(right.getSchema());
		idx = 0;
		leftTuple = left.getNextTuple();
	}
	
	/**
	 * Retreives the tables that are being joined by this joinOperator
	 * @return A string delimited by commas telling us all the tables being joined.
	 */
	public String getTables() {
		return tables;
	}

	/**
	 * Retrieves the where condition.
	 *
	 * @return An expression or null.
	 */
	public Expression getWhere() {
		return where;
	}

	/**
	 * Retrieves the schema information.
	 *
	 * @return An arraylist representing the schema.
	 */
	public ArrayList<String> getSchema() {
		return schema;
	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
//		if (idx == results.size()) {
//			return null;
//		}
//		return new Tuple(results.get(idx++).toString());
		if (leftTuple == null) { // no more tuples to join
			return null;
		}
		Tuple rightTuple = right.getNextTuple();
		if (rightTuple == null) {
			right.reset();
			rightTuple = right.getNextTuple();
			leftTuple = left.getNextTuple();
		}
		if (leftTuple == null) { // no more tuples to join
			return null;
		}
		ArrayList<String> together = leftTuple.getTuple();
		together.addAll(rightTuple.getTuple());
		Tuple joinedTuple = new Tuple(together);
		
		if (where == null) {
			return joinedTuple;
		} else {
			ExpressionEvaluator eval = new ExpressionEvaluator(joinedTuple, schema);
			where.accept(eval);
			if (Boolean.parseBoolean(eval.getValue())) {
				return joinedTuple;
			} else {
				// Some strange behavior going on here. The joinedTuple is always 
				// 2 longer than the previous one when it should not be.
				System.out.println(joinedTuple);
				return getNextTuple();
			}
		}
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	@Override
	public void reset() {
		idx = 0;
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	@Override
	public void dump() {
		Tuple temp = this.getNextTuple();
		while (temp != null) {
			System.out.println(temp);
			temp = this.getNextTuple();
		}
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	@Override
	public void dump(String outputFile) {
		Tuple nextTuple = getNextTuple();
		try {
			BinaryTupleWriter out = new BinaryTupleWriter(outputFile);
			while (nextTuple != null) {
				out.writeTuple(nextTuple);
				nextTuple = getNextTuple();
			}
			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}

	/**
	 * Returns the root of the JoinOperatorTree.
	 *
	 * @return A tree representing the order of joins.
	 */
	public JoinOperatorTree getRoot() {
		return tree;
	}

}
