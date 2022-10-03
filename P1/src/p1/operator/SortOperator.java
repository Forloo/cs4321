package p1.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import p1.io.BinaryTupleWriter;
import p1.util.Tuple;

/**
 * An operator that sorts rows based on the ORDER BY clause by using a custom
 * comparator that compares based on the specified ORDER BY columns.
 */
public class SortOperator extends Operator {

	// The child operator (join, project, select, or scan)
	private Operator child = null;
	// The list of column names corresponding to the returned rows.
	private ArrayList<String> schema = new ArrayList<String>();
	// A list of columns to sort by.
	private ArrayList<String> orderBy = new ArrayList<String>();
	// The index of the orderBy columns in schema.
	private ArrayList<Integer> orderByIdx = new ArrayList<Integer>();
	// The returned rows from the child operator to sort.
	private ArrayList<Tuple> tupleData = new ArrayList<Tuple>();
	// The index of the row we are currently looking at in getNextTuple().
	private int idx = 0;

	/**
	 * Determines the orderBy list and selects a child operator.
	 *
	 * @param op     the child operator
	 * @param orders the columns to order by
	 */
	public SortOperator(Operator op, List orders) {
		child = op;
		schema = op.getSchema();
		
		// Get the rows from the child operator
		Tuple currTuple = child.getNextTuple();
		while (currTuple != null) {
			tupleData.add(currTuple);
			currTuple = child.getNextTuple();
		}
		// Get a list of columns to order by
		if (orders != null) {
			for (Object c : orders) {
				orderBy.add(c.toString());
			}
			// Add the rest of the columns to break ties
			for (String col : schema) {
				if (!orderBy.contains(col)) {
					orderBy.add(col);
				}
			}
		} else {
			orderBy = schema;
		}

		// Get the indices of columns to order by
		for (String col : orderBy) {
			orderByIdx.add(schema.indexOf(col));
		}
		
		Collections.sort(tupleData, new CompareTuples());
	}

	/**
	 * A custom Comparator that sorts two Tuples based on the orderBy columns.
	 */
	public class CompareTuples implements Comparator<Tuple> {

		@Override
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
			for (Integer i : orderByIdx) {
				int t1 = Integer.valueOf(o1.getTuple().get(i));
				int t2 = Integer.valueOf(o2.getTuple().get(i));
				if (t1 < t2) {
					return -1;
				} else if (t1 > t2) {
					return 1;
				}
			}

			return 0;
		}

	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		if (idx == tupleData.size()) {
			return null;
		}
		return tupleData.get(idx++);
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		idx = 0;
		child.reset();
	}

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the scan table.
	 */
	public ArrayList<String> getSchema() {
		return schema;
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
