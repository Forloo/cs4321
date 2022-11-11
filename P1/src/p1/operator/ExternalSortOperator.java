package p1.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import p1.io.BinaryTupleReader;
import p1.io.BinaryTupleWriter;
import p1.io.FileConverter;
import p1.util.Tuple;

/**
 * Physical external sort operator
 */
public class ExternalSortOperator extends Operator {

	private Operator child = null;
	private ArrayList<String> schema = new ArrayList<String>();
	private BinaryTupleReader reader = null;
	private int bufferPages;
	public static final int pageSize = 4096;
	private List<String> order;
	private ArrayList<Integer> orderByIdx = new ArrayList<Integer>();
	private String tempDir;
	private String finalFile;

	/**
	 * Constructor for the operator.
	 *
	 * @param op         the child operator
	 * @param list       the sorting order
	 * @param bufferSize The number of pages that will be used
	 * @param tempDir    the file path for temporary directory
	 */
	public ExternalSortOperator(Operator op, List<String> list, int bufferSize, String tempDirPath, int id) {
		Random rand = new Random();
		id = rand.nextInt(1000000);
		tempDir = tempDirPath + id;
		child = op;
		schema = child.getSchema();
		bufferPages = bufferSize;
		order = list;

		// Get the indices of columns to order by
		for (String col : order) {
			orderByIdx.add(schema.indexOf(col));
		}
		for (int i = 0; i < schema.size(); i++) {
			if (!orderByIdx.contains(i)) {
				orderByIdx.add(i);
			}
		}
		try {
			sort();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set name of temp file for each run
	 * 
	 * @param pass
	 * @param run
	 */
	public String nameTempFile(int pass, int run) {
		return tempDir + "Pass_" + Integer.toString(pass) + "Run_" + Integer.toString(run);
	}

	/**
	 * Create number of runs, sort each run
	 */
	public void sort() throws IOException {
		int tuplesPerPage = 4088 / schema.size() / 4;
		int totalTuples = tuplesPerPage * bufferPages;

		int run = 0;
		Tuple tup = null;
		ArrayList<String> fileList = new ArrayList<String>();

		ArrayList<Tuple> sortList = new ArrayList<Tuple>();

		while (true) {
			sortList.clear();
			int tuplesRemaining = totalTuples;

			while (tuplesRemaining-- > 0 && (tup = child.getNextTuple()) != null) {
				sortList.add(tup);
			}

			if (sortList.size() == 0)
				break;

			Collections.sort(sortList, new CompareTuples());

			String fileName = nameTempFile(0, run);
			BinaryTupleWriter writer = new BinaryTupleWriter(fileName);
			fileList.add(fileName);

			for (Tuple t : sortList) {
				writer.writeTuple(t);
			}
			writer.close();

			// for debugging
			FileConverter.convertBinToHuman(fileName, fileName + "_humanreadable");

			run++;
		}

		merge(run, bufferPages, fileList, tuplesPerPage);
	}

	/**
	 * Merge the runs
	 * 
	 * @param n             is the number of sorted runs left to merge into one big
	 *                      run
	 * @param b             is the size of buffer
	 * @param fileList      is the list of file names in temp directory to use
	 * @param tuplesPerPage represents the number of tuples that fills the output
	 *                      buffer for each step of merge.
	 */
	public void merge(int n, int b, List<String> fileList, int tuplesPerPage) {
		int numRunsAfterMerge = fileList.size() * (b - 1);// issue is when we start with n number of runs where n is not
															// a power of 2
		tuplesPerPage = tuplesPerPage * b;// initialize output buffer size or run size of merge step
		int ms = 0; // merge step for storing files in temp dir
		int rn = 0; // n'th run for storing files in temp dir

		// this many steps of merge
		int totalMerge = (int) (Math.log(fileList.size()) / Math.log(b - 1)) + 1;
		for (int i = 0; i < totalMerge; i++) {

			// for merging into one big run
			if (numRunsAfterMerge % (b - 1) != 0) {
				numRunsAfterMerge = numRunsAfterMerge / (b - 1) + 1; // same issue as mentioned
			} else {
				numRunsAfterMerge = numRunsAfterMerge / (b - 1); // same issue as mentioned
			}

			// adjust fileList here, potential issue here
			List<String> subItems = new ArrayList<String>(fileList.subList(0, numRunsAfterMerge)); // change

			// initialize all runs
			ArrayList<BinaryTupleReader> fileReaders = new ArrayList<BinaryTupleReader>();
			for (String f : subItems) {
				// make BinaryTupleReader for all runs...
				fileReaders.add(new BinaryTupleReader(f));
			}

			// initialize input buffer (inputBufferRun) (load the tuples)
			ArrayList<BinaryTupleReader> inputBufferRun = new ArrayList<BinaryTupleReader>();
			ArrayList<Tuple> inputBufferTuple = new ArrayList<Tuple>();
			int numTupInBuff = 0;
			for (BinaryTupleReader fileRead : fileReaders) {
				if (numTupInBuff < b - 1) {
					inputBufferTuple.add(fileRead.nextTuple());
					numTupInBuff++;
					inputBufferRun.add(fileRead);
				} else {
					break;
				}

			}

			// create uninputBufferRun (leftover runs to refill input buffer once one runs
			// out)
			ArrayList<BinaryTupleReader> uninputBufferRun = new ArrayList<BinaryTupleReader>();
			int copy = numTupInBuff;

			if (copy == b - 1) { // when equal amount of tuples in input buff, there might be left over runs...
				try {
					while (copy <= fileReaders.size() - 1) {
						uninputBufferRun.add(fileReaders.get(copy));
						copy++;
					}
				} catch (Exception e) {
					continue;
				}

			}

			int outBufferNumTup = 0; // to check how many tuples in output buffer now
//			int currentNumRuns = 0; //to check current merge step's number of produced runs
			tuplesPerPage = tuplesPerPage * (b - 1);


			ArrayList<Tuple> outputBuffer = new ArrayList<Tuple>(); // intialize output buffer
			ms++; // for storing files, creating unique names
			rn = 0;
			while (!inputBufferTuple.isEmpty()) { // 1 merge step
				// finding min tuple, saving which input buffer page min came from

				Tuple minTup = null;
				CompareTuples tc = new CompareTuples();
				int minTupIndx = 0;
				int inc = 0;
				for (Tuple tup : inputBufferTuple) {
					if (minTup == null) {
						minTup = tup;
						minTupIndx = inc;
					} else if (tc.compare(tup, minTup) == -1) {
						minTup = tup;
						minTupIndx = inc;
					}

					inc++;
				}

				// writing to output buffer
				outputBuffer.add(minTup);
				outBufferNumTup++;

				// updating the input buffer
				Tuple nextTupInBuff = inputBufferRun.get(minTupIndx).nextTuple();
				if (nextTupInBuff == null) {// if one input buffer runs out, just remove
					inputBufferRun.remove(minTupIndx);
					inputBufferTuple.remove(minTupIndx);

				} else { // else just keep getting next tuple from the same run
					inputBufferTuple.set(minTupIndx, nextTupInBuff);
				}
				// write the disk and clear output buffer
				if (outBufferNumTup == tuplesPerPage || inputBufferTuple.isEmpty()) {
					String fileName = tempDir + "mergeStep_" + Integer.toString(ms) + "_run_" + Integer.toString(rn);
					fileList.add(0, fileName); 

					rn++; // creating unique names

					// write to temp dir
					BinaryTupleWriter writer = new BinaryTupleWriter(fileName);
					finalFile = fileName;
					reader = new BinaryTupleReader(fileName);
					for (Tuple t : outputBuffer) {
						if (t == null) {
							break;
						}
						writer.writeTuple(t);
					} // does this pad the binary file with zeros?
					writer.close();
					FileConverter.convertBinToHuman(fileName, fileName + "_humanreadable"); // for debugging
					outputBuffer = new ArrayList<Tuple>();// reset output buffer
					outBufferNumTup = 0;// reset output buffer (for efficiency)
				}

				// added code
				if (inputBufferRun.isEmpty() && !uninputBufferRun.isEmpty()) { // done sorting input buffer worth runs
					// now re-initialize input buffer and tuples
					int numTupInBuff2 = 0;
					for (BinaryTupleReader btr : uninputBufferRun) {
						if (numTupInBuff2 < b - 1 && !uninputBufferRun.isEmpty()) {
							inputBufferRun.add(btr);
							inputBufferTuple.add(inputBufferRun.get(numTupInBuff2).nextTuple());
							numTupInBuff2++;
						} else {
							break;
						}
					}
					// now remove b-1 buffers from uninputBufferRun
					for (int i2 = 0; i2 < b - 1; i2++) {
						if (!uninputBufferRun.isEmpty()) {
							uninputBufferRun.remove(0);
						}
					}

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
		return reader.nextTuple();
	}

	/**
	 * Tells the operator to reset its state and start reading its output again from
	 * the beginning
	 */
	@Override
	public void reset() {
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
		Tuple nextTuple = reader.nextTuple();
		while (nextTuple != null) {
			System.out.println(nextTuple.toString());
			nextTuple = reader.nextTuple();
		}
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	@Override
	public void dump(String outputFile) {
		Tuple nextTuple = reader.nextTuple();
		try {
			BinaryTupleWriter out = new BinaryTupleWriter(outputFile);
			while (nextTuple != null) {
				out.writeTuple(nextTuple);
				nextTuple = reader.nextTuple();
			}
			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}

}
