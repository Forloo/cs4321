package p1.operator;

import p1.io.BinaryTupleReader;
import p1.io.BinaryTupleWriter;
import p1.util.DatabaseCatalog;
import p1.util.Tuple;

/**
 * An operator that opens a file scan on the appropriate data file to return all
 * rows in that table.
 */
public class ScanOperator extends Operator {

	// Binary file reader.
	BinaryTupleReader reader;

	/**
	 * Constructor to scan rows of table fromTable.
	 */
	public ScanOperator(String fromTable) {
//		rows = new ArrayList<String>();
//		idx = 0;
//		try {
//			String fileLoc = DatabaseCatalog.getInstance().getNames().get(fromTable);
//			File file = new File(fileLoc);
//			Scanner fileReader = new Scanner(file);
//			while (fileReader.hasNextLine()) {
//				rows.add(fileReader.nextLine());
//			}
//			fileReader.close();
//		} catch (FileNotFoundException e) {
//			System.out.println("An error occurred.");
//			e.printStackTrace();
//		}
		reader = new BinaryTupleReader(DatabaseCatalog.getInstance().getNames().get(fromTable));
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
