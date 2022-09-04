package p1.operator;

import java.io.PrintWriter;

import p1.Tuple;

/**
 * An operator that opens a file scan on the appropriate data file.
 */
public class ScanOperator extends Operator {

	/**
	 * Constructor to scan rows of table fromTable.
	 */
	public ScanOperator(String fromTable) {

	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return new Tuple("0,0,0");
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		// TODO Auto-generated method stub

	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	public void dump() {
//		while () {
		System.out.println(getNextTuple().toString());
//		}
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	public void dump(String outputFile) {
		try {
			PrintWriter out = new PrintWriter(outputFile);

//			while () {
			out.println(getNextTuple().toString());
//			}

			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}

}
