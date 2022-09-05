package p1.operator;

import java.io.PrintWriter;
import java.util.ArrayList;

import p1.Tuple;

public class SelectOperator extends Operator {

	// rows of the table
	private ArrayList<String> rows;
	// index of next item/tuple
	private int idx;

	/**
	 * Retrieves the next tuples matching the selection condition. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		return null;
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {

	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	public void dump() {
		while (idx < rows.size()) {
			System.out.println(getNextTuple().toString());
		}
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

			while (idx < rows.size()) {
				out.println(getNextTuple().toString());
			}

			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}

}
