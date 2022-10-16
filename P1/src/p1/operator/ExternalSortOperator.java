package p1.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import p1.io.BinaryTupleReader;
import p1.io.BinaryTupleWriter;
import p1.util.DatabaseCatalog;
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
	private String tempDir;
	
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
		schema = child.getSchema();
		bufferPages = bufferSize;
		order = orders;
		
		// Get the indices of columns to order by
		for (String col : order) {
			orderByIdx.add(schema.indexOf(col));
		}		
	}
	
	/**
	 * Set name of temp file for each run
	 */
	public String nameTempFile(int pass, int run) {
		return "Pass_" + Integer.toString(pass) + "Run_" + Integer.toString(run);
	}

	/**
	 * Create number of runs, sort each run
	 */
	public void sort() throws IOException {
		int totalTuples = (4096 / schema.size() / 4) * bufferPages;
		int run = 0; 
		Tuple tup;
				
		while ((tup = child.getNextTuple()) != null) {
			List<Tuple> sortList = new ArrayList<>(totalTuples);
			int tuplesRemaining = totalTuples;
			
			while (tuplesRemaining > 0 && tup != null) {
				sortList.add(tup);
				tup = child.getNextTuple();
				tuplesRemaining--;
			}
			
			Collections.sort(sortList, new CompareTuples());
			
			BinaryTupleWriter writer = new BinaryTupleWriter(nameTempFile(0, run));
			
			for(Tuple t : sortList) {
			    writer.writeTuple(t);
			} writer.close();
			
	        run++;
			
	        merge(run, bufferPages, writer, fileList, tuplesPerPage);		
		}	
	}
	
	


/**
 * Merge the runs
 * @param n is the number of sorted runs left to merge into one big run
 * @param b is the size of buffer
 */
public void merge(int n, int b, String temp, List<String> fileList,int tuplesPerPage) {
	//keeps track of which files to read and put into input buffer
	ArrayList<BinaryTupleReader> fileReaders = new ArrayList<BinaryTupleReader>();
	for (String f : fileList) {
		//make BinaryTupleReader for all runs...
		fileReaders.add(new BinaryTupleReader(f));
	}
	//whenever this many tuples in output buffer, write to disk
	int eachRunSize = tuplesPerPage;
	//whenever write to disk when output buffer is full, add 1 here
	int newTempDirNum = 0;
	while(n!=1) { //to merge to one big run
		//initialize input buffer
		for(int i=0; i<b;i++) {//to add tuples of correct page of runs to each input buffer
			ArrayList<Tuple> onePage = new ArrayList<Tuple>();
			int leftRuns = 0; //find run with pages left
			Tuple saveTuple = fileReaders.get(leftRuns).nextTuple();
			while(saveTuple == null) {
				leftRuns++; //keep adding
				saveTuple = fileReaders.get(leftRuns).nextTuple();
			}
			if (saveTuple != null) {
				onePage.add(saveTuple);
			}
			int t=0;
			while(t<tuplesPerPage) { //add tuples to input buffer
				onePage.add(fileReaders.get(i).nextTuple());
				t++;
			}
			inputBuffer.add(onePage);
		}
		
		//done initializing input buffer
		while(true) {//to merge intermediate runs, each iteration writes to disk
			ArrayList<Tuple> outputBuffer = new ArrayList<Tuple>(); //resets after writing to disk
			int oBTupNum = 0;
			ArrayList<Tuple> sortList = new ArrayList<Tuple>();
			
			Tuple minTup = null;
			//find the minimum in inputBuffer
//			ArrayList<Tuple> combined = new ArrayList<Tuple>();
			int minCoordOut = 0;
			int minCoordIn = 0;
			int outer = 0;
			int inner = 0;
			for(ArrayList<Tuple> aTup : inputBuffer) {
				for(Tuple tup : aTup) {
					if (minTup == null || (compare(tup, minTup) == -1)) {
						minTup = tup;
						minCoordOut = outer;
						minCoordIn = inner;
					}
					inner++;
				}
				outer ++;
			}
			inputBuffer.get(outer).remove(inner); //delete that min node from corresponding page
			//write to new file when sortList is eachRunSize
			if (oBTupNum == eachRunSize) {
				BinaryTupleWriter writer = new BinaryTupleWriter("temp");
				for(Tuple t : sortList) {
				    writer.writeTuple(t);
				}
				writer.close();
			}
			//how should I keep track of what tuple i am at in a certain run's page?
			//add 1 to n after each merge
			n ++;
		}
		
		eachRunSize = eachRunSize * 2; //update when to write to disk from output buffer
		n = n / 2; //after one merge for all runs in temp dir is done, reduce n size
		
	}
	

}

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
