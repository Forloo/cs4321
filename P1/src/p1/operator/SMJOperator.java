package p1.operator;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import p1.io.BinaryTupleWriter;
import p1.util.Tuple;

/**
 * An operator that processes queries on multiple files and conditions.
 */
public class SMJOperator extends Operator {

	// Left child operator
	private SortOperator left;
	// Right child operator
	private SortOperator right;
	// The schema for the results query table
	private ArrayList<String> schema;
	// The where condition. Only used to call accept.
	private ArrayList<Expression> where;
	// The left tuple to join with.
	private Tuple leftTuple;
	// The right tuple to join with.
	private Tuple rightTuple;
	// The tables that are being joined on by this joinoperator
	private String tables;

	/**
	 * Creates a JoinOperatorTree.
	 *
	 * @param left  the left child operator
	 * @param right the right child operator
	 * @param exp   The where expression will be passed in by the query plan.
	 */
	public SMJOperator(String tables, SortOperator left, SortOperator right, ArrayList<Expression> exp) {
		this.left = left;
		this.right = right;
		this.tables = tables;

		where = exp;
		ArrayList<String> schema2 = new ArrayList<String>();
		schema2.addAll(left.getSchema());
		schema2.addAll(right.getSchema());
		schema = schema2;
		leftTuple = left.getNextTuple();
		rightTuple = right.getNextTuple();
	}

	/**
	 * Retreives the tables that are being joined by this joinOperator
	 *
	 * @return A string delimited by commas telling us all the tables being joined.
	 */
	public String getTable() {
		return tables;
	}

	/**
	 * Retrieves the where condition.
	 *
	 * @return An expression or null.
	 */
	public ArrayList<Expression> getWhere() {
		return where;
	}

	public SortOperator getLeft() {
		return left;
	}

	public SortOperator getRight() {
		return right;
	}

	/**
	 * Retrieves the schema information.
	 *
	 * @return An arraylist representing the schema.
	 */
	public ArrayList<String> getSchema() {
		return schema;
	}

	public Tuple getLeftTuple() {
		return leftTuple;
	}

	public void setLeftTuple(Tuple leftValue) {
		this.leftTuple = leftValue;
	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
//		if (leftTuple == null) { // no more tuples to join
//			return null;
//		}
//		Tuple rightTuple = right.getNextTuple();
//		if (rightTuple == null) {
//			right.reset();
//			rightTuple = right.getNextTuple();
//			leftTuple = left.getNextTuple();
//		}
//		if (leftTuple == null) { // no more tuples to join
//			return null;
//		}
//		ArrayList<String> together2 = new ArrayList<String>();
//		together2.addAll(leftTuple.getTuple());
//		together2.addAll(rightTuple.getTuple());
//		Tuple joinedTuple = new Tuple(together2);
//
//		if (where == null) {
//			return joinedTuple;
//		} else {
//			ExpressionEvaluator eval = new ExpressionEvaluator(joinedTuple, schema);
//			// There must be at least one expression if we enter this loop
//			boolean allTrue = true;
//			for (int i = 0; i < this.getWhere().size(); i++) {
//				this.getWhere().get(i).accept(eval);
//				allTrue = allTrue && (Boolean.parseBoolean(eval.getValue()));
//			}
//			if (allTrue) {
//				return joinedTuple;
//			} else {
//				return getNextTuple();
//			}
//		}
		while (leftTuple != null && rightTuple != null) {
//			if (leftTuple > rightTuple) {
//				rightTuple = right.getNextTuple();
//			} else if (leftTuple < rightTuple) {
//				leftTuple = left.getNextTuple();
//			} else {
//				ArrayList<String> together = new ArrayList<String>();
//				together.addAll(leftTuple.getTuple());
//				together.addAll(rightTuple.getTuple());
//				return new Tuple(together);
//			}
		}
		return null;
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	@Override
	public void reset() {
		left.reset();
		right.reset();
		Tuple leftValue = this.getLeft().getNextTuple();
		this.setLeftTuple(leftValue);
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

}
