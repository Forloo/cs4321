package p1.operator;

import java.util.ArrayList;
import java.util.List;

import p1.io.BinaryTupleWriter;
import p1.util.Tuple;

/**
 * An operator that returns selected columns based on projection requirements.
 * We index the columns that should be returned and only return those indices
 * from the Tuple ArrayList.
 */
public class ProjectOperator extends Operator {
	// The child operator.
	private Operator child;
	// A list of all columns.
	private ArrayList<String> schema;
	// The columns of the rows to return.
	private ArrayList<String> cols;
	// The index of the row we are currently checking/returning.
	int idx = 0;

	/**
	 * Initializes the variables above.
	 *
	 * @param op      the child operator
	 * @param selects the columns to project
	 */
	public ProjectOperator(Operator op, List selects) {
		schema = op.getSchema();
		child = op;

		cols = new ArrayList<String>();
		for (int i = 0; i < selects.size(); i++) {
			cols.add(selects.get(i).toString());
		}
	}

	/**
	 * Retrieves the next tuples matching the selection condition. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		Tuple nextTuple = child.getNextTuple();

		if (nextTuple == null) {
			return null;
		}

		ArrayList<String> projection = new ArrayList<>();

		for (String i : cols) {
			int idx = schema.indexOf(i);
			projection.add(nextTuple.getTuple().get(idx));
		}
		return new Tuple(projection);
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		child.reset();
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
		return cols;
	}

	/**
	 * Gets the table name.
	 *
	 * @return the table name.
	 */
	public String getTable() {
		return child.getTable();
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

	/**
	 * Gets the string to print for the physical plan
	 * 
	 * @param level the level of the operator
	 * @return the physical plan in string form
	 */
	public String toString(int level) {
		return "-".repeat(level) + "Project" + cols.toString() + "\n" + child.toString(level + 1);
	}
}
