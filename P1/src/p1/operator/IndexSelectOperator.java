package p1.operator;

TODO

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import p1.io.BinaryTupleWriter;
import p1.util.ExpressionEvaluator;
import p1.util.Tuple;

/**
 * This operator selects rows based on a where condition. Add the tuple to the
 * output if ExpressionVisitor determines that the condition is true, and skip
 * the tuple if not.
 */
public class IndexSelectOperator extends Operator {
	// The child operator, scanning all rows.
	private Operator scanObj;
	// The expression to check rows on.
	private Expression where;

	/**
	 * Determines selection conditions and rows.
	 *
	 * @param op the child scan operator.
	 * @param ex the expression to select tuples from.
	 */
	public IndexSelectOperator(Operator op, Expression ex) {
		where = ex;
		scanObj = op;
	}

	/**
	 * Retrieves the next tuples matching the selection condition. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		while (true) {
			Tuple nextTuple = scanObj.getNextTuple();
			if (nextTuple == null) {
				return null;
			}

			ExpressionEvaluator exprObj2 = new ExpressionEvaluator(nextTuple, scanObj.getSchema());
			where.accept(exprObj2);
			if (Boolean.parseBoolean(exprObj2.getValue())) {
				return nextTuple;
			}
		}
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		scanObj.reset();
	}

	/**
	 * Resets the Operator to the ith tuple.
	 *
	 * @param idx the index to reset the Operator to
	 */
	public void reset(int idx) {
	}

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the scan table.
	 */
	public ArrayList<String> getSchema() {
		return scanObj.getSchema();
	}

	/**
	 * Gets the table name.
	 *
	 * @return the table name.
	 */
	public String getTable() {
		return scanObj.getTable();
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	public void dump() {
		Tuple nextTuple = getNextTuple();
		while (nextTuple != null) {
			System.out.println(nextTuple.toString());
			nextTuple = getNextTuple();
		}
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
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