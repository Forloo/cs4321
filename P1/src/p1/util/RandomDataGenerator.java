package p1.util;

import java.util.ArrayList;
import java.util.Random;

/**
 * Generates a specified number of random tuples of the same size.
 */
public class RandomDataGenerator {

	// The randomly generated tuples.
	private ArrayList<Tuple> tuples;
	// The number of columns/attributes.
	private int numCol;
	// The number of rows/tuples.
	private int numRow;

	/**
	 * Sets the number of attributes and rows for the tuple list.
	 *
	 * @param numAttr   the number of columns.
	 * @param numTuples the number of tuples.
	 */
	public RandomDataGenerator(int numAttr, int numTuples) {
		numCol = numAttr;
		numRow = numTuples;
	}

	/**
	 * Generates the specified number of tuples.
	 *
	 * @return an ArrayList of Tuples.
	 */
	public ArrayList<Tuple> generate() {
		tuples = new ArrayList<Tuple>();
		Random rand = new Random();
		for (int i = 0; i < numRow; i++) {
			String tuple = "";
			for (int j = 0; j < numCol - 1; j++) {
				tuple += rand.nextInt(1000) + ",";
			}
			tuples.add(new Tuple(tuple + rand.nextInt(1000)));
		}
		return tuples;
	}
}
