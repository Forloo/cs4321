package p1.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import p1.io.BinaryTupleReader;
import p1.io.BinaryTupleWriter;
import p1.io.HumanTupleReader;
import p1.io.HumanTupleWriter;

/**
 * Utility that outputs sorted files after taking in either file formats
 */
public class SortFile {

	/**
	 * Reads from appropriate file types
	 *
	 * @param fileOut the file to write sorted tuples and output
	 * @param binary  true if file is binary format false if human readable format
	 * @throws IOException
	 */

	private ArrayList<Tuple> listTuple; // list of tuples read from file to be sorted

	private String fileOutt; // file to write outputs

	public SortFile(String fileOut, Boolean binary) {
		fileOutt = fileOut; // changes file out which is the file to overwrite after sorting
		listTuple = new ArrayList<Tuple>();
		if (binary) {
			BinaryTupleReader btr = new BinaryTupleReader(fileOut);
			Tuple nextTup = btr.nextTuple();
			while (nextTup != null) { // add to listTuple the contents of the file
				listTuple.add(nextTup);
				nextTup = btr.nextTuple();
			}
		} else {
			HumanTupleReader htr = new HumanTupleReader(fileOut);
			Tuple nextTup = htr.nextTuple();
			while (nextTup != null) { // add to listTuple the contents of the file
				listTuple.add(nextTup);
				nextTup = htr.nextTuple();
			}
		}
	}

	/**
	 * Overwrites the output file in binary format after sorting the tuples
	 */
	public void sortBinary() {
		Collections.sort(listTuple, new CompareTuples());
		BinaryTupleWriter btw = new BinaryTupleWriter(fileOutt);
		for (int i = 0; i < listTuple.size(); i++) { // for each tuple write
			btw.writeTuple(listTuple.get(i));
		}
		btw.close();
	}

	/**
	 * Overwrites the output file in human readable format after sorting the tuples
	 */
	public void sortHuman() {
		Collections.sort(listTuple, new CompareTuples());
		HumanTupleWriter htr = new HumanTupleWriter(fileOutt);
		for (int i = 0; i < listTuple.size(); i++) { // for each tuple write
			htr.writeTuple(listTuple.get(i));
		}
		htr.close();
	}

	/**
	 * A custom Comparator that sorts two Tuples starting from first column.
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
			for (Integer i = 0; i < o1.getTuple().size(); i++) {
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
}