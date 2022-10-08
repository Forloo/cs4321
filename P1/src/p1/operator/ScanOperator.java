package p1.operator;

import java.util.ArrayList;

import p1.io.BinaryTupleReader;
import p1.io.BinaryTupleWriter;
import p1.util.Aliases;
import p1.util.DatabaseCatalog;
import p1.util.Tuple;

/**
 * An operator that opens a file scan on the appropriate data file to return all
 * rows in that table.
 */
public class ScanOperator extends Operator {

	// Binary file reader.
	BinaryTupleReader reader;
	// Column names.
	ArrayList<String> schema;
	// Table name.
	String table;

	/**
	 * Constructor to scan rows of table fromTable (aliased).
	 */
	public ScanOperator(String fromTable) {
		reader = new BinaryTupleReader(DatabaseCatalog.getInstance().getNames().get(Aliases.getTable(fromTable)));
		ArrayList<String> newSchema = new ArrayList<String>();
		for (String col : DatabaseCatalog.getInstance().getSchema().get(Aliases.getTable(fromTable))) {
			String[] els = col.split("\\.");
			String colName = els[els.length - 1];
			newSchema.add(fromTable + "." + colName);
		}
		schema = newSchema;
		table = fromTable;
	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		return reader.nextTuple();
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		reader.reset();
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
		return schema;
	}

	/**
	 * Gets the table name.
	 *
	 * @return the table name.
	 */
	public String getTable() {
		return table;
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	public void dump() {
		Tuple next = getNextTuple();
		while (next != null) {
			System.out.println(next.toString());
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
