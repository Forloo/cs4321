package p1.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import p1.io.BinaryTupleWriter;
import p1.util.DatabaseCatalog;
import p1.util.ExpressionEvaluator;
import p1.util.Tuple;

/**
 * An operator that processes queries on multiple files and conditions using the
 * sort-merge-join algorithm.
 */
public class SMJOperator extends Operator {

	// Left child operator
	private Operator left;
	// Right child operator
	private Operator right;
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
	// Sort order for left child
	private List<String> leftOrder;
	// Sort order for right child
	private List<String> rightOrder;
	// Compare order: [[S.A, R.G], [S.B, R.H], ...]
	private ArrayList<String[]> compareOrder;
	// Start of the current partition
	private int pIdx;
	// Current partition index
	private int idx;

	/**
	 * Creates a JoinOperatorTree.
	 *
	 * @param tables the names of the tables to join, separated by a comma
	 * @param left   the left child operator
	 * @param right  the right child operator
	 * @param exp    the where expression will be passed in by the query plan.
	 */
	public SMJOperator(String tables, Operator leftOp, Operator rightOp, ArrayList<Expression> exp) {
		leftOrder = new ArrayList<String>();
		rightOrder = new ArrayList<String>();
		compareOrder = new ArrayList<String[]>();
		String[] leftTables = leftOp.getTable().split(",");
		String[] rightTables = rightOp.getTable().split(",");
		Arrays.sort(leftTables);
		Arrays.sort(rightTables);

		// Get sort order for child sort operators
		for (Expression e : exp) {
			String[] condition = e.toString().split(" ");
			if (leftOp.getSchema().contains(condition[0]) && rightOp.getSchema().contains(condition[2])) {
				if (!leftOrder.contains(condition[0]))
					leftOrder.add(condition[0]);
				if (!rightOrder.contains(condition[2]))
					rightOrder.add(condition[2]);
			}
		}

		// Get order for comparisons to determine "less than", "greater than"
		for (Expression e : exp) {
			String[] conditions = e.toString().split(" ");
			if (conditions[0].contains(".") && conditions[2].contains(".")) { // make sure not S.A < 5
				compareOrder.add(new String[] { conditions[0], conditions[2] });
			}
		}

		if (DatabaseCatalog.getInstance().getSortMethod() == 0) { // in-memory sort
			left = new SortOperator(leftOp, leftOrder);
			right = new SortOperator(rightOp, rightOrder);
		} else { // external sort
			left = new ExternalSortOperator(leftOp, leftOrder, DatabaseCatalog.getInstance().getSortPages(),
					DatabaseCatalog.getInstance().getTempDir(), 0);
			right = new ExternalSortOperator(rightOp, rightOrder, DatabaseCatalog.getInstance().getSortPages(),
					DatabaseCatalog.getInstance().getTempDir(), 1);
		}
		this.tables = tables;

		where = exp;
		ArrayList<String> schema2 = new ArrayList<String>();
		schema2.addAll(left.getSchema());
		schema2.addAll(right.getSchema());
		schema = schema2;
		leftTuple = left.getNextTuple();
		rightTuple = right.getNextTuple();
		pIdx = 0;
		idx = 0;
	}

	/**
	 * Retrieves the tables that are being joined by this joinOperator
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
		while (leftTuple != null && rightTuple != null) {
			if (compare(leftTuple, rightTuple) == -1) {
				leftTuple = left.getNextTuple();
				continue;
			}
			if (compare(leftTuple, rightTuple) == 1) {
				rightTuple = right.getNextTuple();
				idx++;
				pIdx = idx;
				continue;
			}

			ArrayList<String> together = new ArrayList<String>();
			together.addAll(leftTuple.getTuple());
			together.addAll(rightTuple.getTuple());
			Tuple joinedTuple = new Tuple(together);
			ExpressionEvaluator eval = new ExpressionEvaluator(joinedTuple, schema);
			boolean allTrue = true;
			for (int i = 0; i < this.getWhere().size(); i++) {
				this.getWhere().get(i).accept(eval);
				allTrue = allTrue && (Boolean.parseBoolean(eval.getValue()));
			}

			rightTuple = right.getNextTuple();
			idx++;

			if (rightTuple == null || compare(leftTuple, rightTuple) != 0) { // reset right partition
				right.reset(pIdx);
				idx = pIdx;
				leftTuple = left.getNextTuple();
				rightTuple = right.getNextTuple();
			}

			if (allTrue) { // if left and right tuple meet join condition
				return joinedTuple;
			}
		}
		return null;
	}

	/**
	 * Compares two Tuples.
	 *
	 * @param o1 the first Tuple to compare.
	 * @param o2 the second Tuple to compare.
	 * @return -1 if the first Tuple should come before the second Tuple, 0 if they
	 *         are equal, and 1 if the first Tuple should come after the second
	 *         Tuple.
	 */
	public int compare(Tuple o1, Tuple o2) {
		for (String[] cols : compareOrder) {
			if (left.getSchema().contains(cols[0]) && right.getSchema().contains(cols[1])) {
				int t1 = Integer.valueOf(o1.getTuple().get(left.getSchema().indexOf(cols[0])));
				int t2 = Integer.valueOf(o2.getTuple().get(right.getSchema().indexOf(cols[1])));
				if (t1 < t2) {
					return -1;
				} else if (t1 > t2) {
					return 1;
				}
			} else if (left.getSchema().contains(cols[1]) && right.getSchema().contains(cols[0])) {
				int t1 = Integer.valueOf(o1.getTuple().get(left.getSchema().indexOf(cols[1])));
				int t2 = Integer.valueOf(o2.getTuple().get(right.getSchema().indexOf(cols[0])));
				if (t1 < t2) {
					return -1;
				} else if (t1 > t2) {
					return 1;
				}
			}
		}

		return 0;
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	@Override
	public void reset() {
		left.reset();
		right.reset();
		leftTuple = left.getNextTuple();
		rightTuple = right.getNextTuple();
	}

	/**
	 * Resets the Operator to the ith tuple.
	 *
	 * @param idx the index to reset the Operator to
	 */
	public void reset(int i) {
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
