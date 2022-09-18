package p1.operator;

import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.io.BinaryTupleWriter;
import p1.util.Tuple;

/**
 * This operator removes duplicate rows by sorting all rows and calling
 * getNextTuple() repeatedly until the next tuple is not the same as the
 * previous tuple.
 */
public class DuplicateEliminationOperator extends Operator {

	// The child operator; if there is no ORDER BY, we order by the original column
	// order and join order.
	private SortOperator child;
	// The previous tuple returned to output.
	private Tuple prev;

	/**
	 * Creates the child sort operator.
	 *
	 * @param ps        the query
	 * @param fromTable the first table to select from
	 */
	public DuplicateEliminationOperator(PlainSelect ps, String fromTable) {
		child = new SortOperator(ps, fromTable);
	}

	/**
	 * Retrieves the next distinct tuples by checking if the current tuple is equal
	 * to the previous tuple. If there is no next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple next = child.getNextTuple();
		if (next == null) {
			return null;
		}
		if (prev == null) {
			prev = next;
			return next;
		}

		if (next.toString().equals(prev.toString())) {
			return getNextTuple();
		}
		prev = next;
		return next;
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		child.reset();
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
