package p1.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import p1.io.BinaryTupleReader;
import p1.io.BinaryTupleWriter;
import p1.util.Tuple;

/**
 * Physical external sort operator
 */
public class ExternalSortOperator extends Operator {

	private Operator child = null;
	private ArrayList<String> schema = new ArrayList<String>();
	private BinaryTupleReader reader = null;
	private int bufferPages;
	public static final int pageSize=4096;
	private ArrayList<String> order;
	private ArrayList<Integer> orderByIdx = new ArrayList<Integer>();
	
	//each stores buffer page worth of tuples from each run
	private ArrayList<ArrayList<Tuple>> inputBuffer = new ArrayList<ArrayList<Tuple>>();
	private ArrayList<ArrayList<Tuple>> outputBuffer = new ArrayList<ArrayList<Tuple>>();

	/**
	 * Constructor for the operator.
	 *
	 * @param op     the child operator
	 * @param orders the sorting order
	 * @param bufferSize The number of pages that will be used 
	 */
	public ExternalSortOperator(Operator op, ArrayList<String> orders, int bufferSize) {
		child = op;
		schema = op.getSchema();
		bufferPages = bufferSize;
		order = orders;
		
		// Get the indices of columns to order by
		for (String col : order) {
			orderByIdx.add(schema.indexOf(col));
		}

	}

	/**
	 * Create number of runs, sort each run
	 */
	public void sort() {
		int tuplesPerPage = 4096 / schema.size() / 4;
		int totalTuples = tuplesPerPage * bufferPages;
		int run = 0; 
		Tuple tup;
		
		List<Tuple> sortList = new ArrayList<>(totalTuples);
		int tuplesRemaining = totalTuples;
		
		while ((tup = child.getNextTuple()) != null) {
			sortList.add(tup);
			tup = child.getNextTuple();
			tuplesRemaining--;
			
		}
		Collections.sort(sortList, new CompareTuples());
		
		BinaryTupleWriter writer = new BinaryTupleWriter("temp");
		for(Tuple t : sortList) {
		    writer.writeTuple(t);
		}
		writer.close();

        run++;
		
		
		merge(run);		
	}

	/**
	 * Merge the runs
	 */
	public void merge(int run) {
		

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
	@Override
	public Tuple getNextTuple() {
		if (reader == null) {
			return null;
		}
		Tuple tp = reader.nextTuple();
		return tp;
	}

	/**
	 * Tells the operator to reset its state and start reading its output again from
	 * the beginning
	 */
	@Override
	public void reset() {
		if (reader == null) {
			return;
		}
		reader.reset();
	}

	/**
	 * Tells the operator to reset its state and start reading its output again from
	 * index i.
	 *
	 * @param idx the index to reset the Operator to
	 */
	@Override
	public void reset(int i) {
		if (reader == null) {
			return;
		}
		reader.reset(i);
	}

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the scan table.
	 */
	@Override
	public ArrayList<String> getSchema() {
		return schema;
	}

	/**
	 * Retreives the tables that are being joined by this joinOperator
	 *
	 * @return A string delimited by commas telling us all the tables being joined.
	 */
	public String getTable() {
		return child.getTable();
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	@Override
	public void dump() {
		// TODO Auto-generated method stub

	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	@Override
	public void dump(String outputFile) {
		// TODO Auto-generated method stub

	}

}
