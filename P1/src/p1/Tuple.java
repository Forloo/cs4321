package p1;

import java.util.ArrayList;

/**
 * A class that represents a row in the database data. One row is represented by
 * an ArrayList.
 */
public class Tuple {

	/**
	 * An ArrayList representing a row, where each element in the ArrayList
	 * represents an individual column item in a row from the table.
	 */
	private ArrayList<String> tuple;

	/**
	 * Constructor to parse a row String into a Tuple.
	 */
	public Tuple(String rowStr) {
		tuple = new ArrayList<String>();
		String[] rowArr = rowStr.split(",");
		for (String item : rowArr) {
			tuple.add(item);
		}
		tuple.toString();
	}

	/**
	 * Get the row tuple.
	 *
	 * @return the ArrayList representation of a row.
	 */
	public ArrayList<String> getTuple() {
		return tuple;
	}

	/**
	 * @return a string representation of this tuple, with items separated by
	 *         commas.
	 */
	public String toString() {
		return String.join(",", tuple);
	}

}
