package p1;

import java.util.ArrayList;

public class Tuple {

	// An ArrayList representing a row, where each element in the ArrayList
	// represents an individual column item in a row from the table.
	private ArrayList<String> tuple;
	
	public Tuple(String rowStr) {
		String[] rowArr = rowStr.split(",");
		for (String item : rowArr) {
			tuple.add(item);
		}
	}
	
	/*
	 * Get the row tuple.
	 * @return the ArrayList representation of a row.
	 */
	public ArrayList<String> getTuple() {
		return tuple;
	}
	
}
