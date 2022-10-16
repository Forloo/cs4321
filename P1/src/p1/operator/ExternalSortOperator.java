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
		int outDirSize = fileList.size() * 2;
		tuplesPerPage = tuplesPerPage / 2;//divide so while loop below works
		while(outDirSize != 1) { //initialize before merge step
			//initialize possible runs to go in input buffer
			ArrayList<BinaryTupleReader> fileReaders = new ArrayList<BinaryTupleReader>();
			for (String f : fileList) {
				//make BinaryTupleReader for all runs...
				fileReaders.add(new BinaryTupleReader(f));
			}
			
			//get the smallest tuple out of each run in input buffer
			ArrayList<Tuple> bMinusOneTuple = new ArrayList<Tuple>();
			for (int i=0;i<b-1;i++) {//add b-1 tuples to find the min
				if(fileReaders.get(i).nextTuple() == null) {
					fileReaders.remove(i);
				} 
				bMinusOneTuple.add(fileReaders.get(i).nextTuple());
			}
			int outBufferNumTup = 0;
			//QUESTION: What is the size of the intermediate run? - twice the original...
			tuplesPerPage = tuplesPerPage * 2;
			int bwOutDirSize = 0;
			outDirSize = outDirSize / 2;
			ArrayList<Tuple> outputBuffer = new ArrayList<Tuple>();
			while(bwOutDirSize != outDirSize) { //merging 1 step (stop merge when outDirSize number of 
				//runs in output temp directory)
				//find the minTup among the tuples added
				Tuple minTup = null;
				CompareTuples tc = new CompareTuples();
				for(Tuple tup : bMinusOneTuple) {
					if (minTup == null || tc.compare(tup,minTup) == -1){
						minTup = tup;
					}
				}
				outputBuffer.add(minTup);
				outBufferNumTup++;
				
				if (outBufferNumTup == tuplesPerPage) { //do we have to create files whenever the 
					//output buffer is full? slide says to write to disk when output buffer is full
					//but I want each files to be intermediate runs, two runs combined, but if i 
					//output whenever output buffer is full, the files will only contain one page, not 
					//runs combined...
					BinaryTupleWriter writer = new BinaryTupleWriter("???");
					bwOutDirSize ++;
					outputBuffer = new ArrayList<Tuple>();//reset output buffer
					for(Tuple t : outputBuffer) {
					    writer.writeTuple(t);
					}
					writer.close();
				}
			}
		}
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
