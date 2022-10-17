package p1.operator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import p1.io.BinaryTupleReader;
import p1.io.BinaryTupleWriter;
import p1.io.FileConverter;
import p1.io.HumanTupleReader;
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
	private List<String> order;
	private ArrayList<Integer> orderByIdx = new ArrayList<Integer>();
	private String tempDir;
	/**
	 * Constructor for the operator.
	 *
	 * @param op     the child operator
	 * @param list the sorting order
	 * @param bufferSize The number of pages that will be used 
	 * @param tempDir the file path for temporary directory
	 */
	public ExternalSortOperator(Operator op, List<String> list, int bufferSize, String tempDirPath, int id) {
		tempDir = tempDirPath + id;
		child = op;
		schema = child.getSchema();
//		System.out.println(child.getSchema().toString()); //debugging 
		bufferPages = bufferSize;
		order = list;
		
		// Get the indices of columns to order by
		for (String col : order) {
			orderByIdx.add(schema.indexOf(col));
		}		
		try {
			sort();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Set name of temp file for each run
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
		int tuplesPerPage = (4096 / schema.size() / 4);
		int totalTuples = tuplesPerPage * bufferPages;
		int run = 0; 
//		System.out.println("tuplesperpage:" + totalTuples);
		Tuple tup;
		List<String> fileList = new ArrayList<String>();
//		System.out.println(child.dump());
		while ((tup = child.getNextTuple()) != null) {
			List<Tuple> sortList = new ArrayList<>(totalTuples);
			int tuplesRemaining = totalTuples;
//			System.out.println(tup.toString());//debug
//			System.out.println(tuplesRemaining);
			while (tuplesRemaining > 0 && tup != null) {
				sortList.add(tup);
				tup = child.getNextTuple();
				tuplesRemaining--;
//				System.out.println(tuplesRemaining);
			}
//			System.out.println(sortList.toString());
			
			Collections.sort(sortList, new CompareTuples());
			
			String fileName = nameTempFile(0, run);
			BinaryTupleWriter writer = new BinaryTupleWriter(fileName); 
			fileList.add(fileName);
//			System.out.println("writing");	
			for(Tuple t : sortList) {
				
			    writer.writeTuple(t);
			} writer.close();
			//for debugging
			FileConverter.convertBinToHuman(fileName, fileName + "_humanreadable");
			

	        run++;	
		}
//		System.out.println(run);
		merge(run, bufferPages, fileList, tuplesPerPage);	
		//for debugging
		for (String s: fileList) {
//			System.out.println(s);
		}
		
	}
	


	/**
	 * Merge the runs
	 * @param n is the number of sorted runs left to merge into one big run
	 * @param b is the size of buffer
	 * @param fileList is the list of file names in temp directory to use
	 * @param tuplesPerPage represents the number of tuples that fills the output buffer for each step of merge.
	 */
	public void merge(int n, int b, List<String> fileList,int tuplesPerPage) {
		
		//are the pages all full?
		//do we always have even number of runs to merge?
		//change file dir to read from 
		//clean temp directory between queries
		int outDirSize = fileList.size() * 2;
//		System.out.println(outDirSize);
		tuplesPerPage = tuplesPerPage / 2;//this is for output buffer
		while(outDirSize != 1) { //initialize before merge step
			//initialize possible runs to go in input buffer
			outDirSize = outDirSize / 2;
			if (outDirSize == 1) {//included here bc once putting under while loop below, code doesn't reach
				break;
			}
			//adjust fileList here
			List<String> subItems = new ArrayList<String>(fileList.subList(0, outDirSize)); //change
			
			
			
			//load to input buffer
			ArrayList<BinaryTupleReader> fileReaders = new ArrayList<BinaryTupleReader>();
			for (String f : subItems) {
				//make BinaryTupleReader for all runs...
				fileReaders.add(new BinaryTupleReader(f));
			}
			
			//get the smallest tuple out of each run in input buffer
			ArrayList<BinaryTupleReader> usedRuns = new ArrayList<BinaryTupleReader>();
			ArrayList<Tuple> bMinusOneTuple = new ArrayList<Tuple>();
			int numTupInBuff = 0;
			for (BinaryTupleReader fileRead : fileReaders) {
				if(numTupInBuff<b-1) {
					bMinusOneTuple.add(fileRead.nextTuple());
					numTupInBuff++;
					usedRuns.add(fileRead);
				} else {
					break;
				}
				
			}
//			for (int i=0;i<b-1;i++) {//add b-1 tuples to find the min
//				if(fileReaders.get(i).nextTuple() == null) {
//					fileReaders.remove(i);
//				} 
//				if(fileReaders.isEmpty()) {//constant time operation, takes care of edge case
//					break;
//				}
//				bMinusOneTuple.add(fileReaders.get(i).nextTuple());
//				numTupInBuff++;
//			}
			ArrayList<BinaryTupleReader> unusedRuns = new ArrayList<BinaryTupleReader>();
			int copy = numTupInBuff;
			if (copy == b-1) { //when equal amount of tuples in input buff, there might be left over runs...
				try {
					while(copy<=fileReaders.size()-1) {
						unusedRuns.add(fileReaders.get(copy)); //keep adding and catch it
						copy++;
					}
				} catch (Exception e){
					continue;
				}
				
			}
			
			System.out.println("fileReader Size: "+fileReaders.size());
			System.out.println("bminus one tuple size: " + bMinusOneTuple.size());
			System.out.println("used run size: "  +usedRuns.size());
			System.out.println("unused run size: "  +unusedRuns.size());
//			while(fileReaders.get(n)) maybe keep a left over queue and pop from it?
			int outBufferNumTup = 0;
			//QUESTION: What is the size of the intermediate run? - twice the original... maybe
			tuplesPerPage = tuplesPerPage * 2;
			int bwOutDirSize = 0;
			
			//for the temp file names
			//Question: Do I need all intermediate temp files? Or can I just overwrite previous files for runs?
			int ms = 0;
			int rn = 0;
//			ArrayList<String> interFileList = new ArrayList<String>();
			ArrayList<Tuple> outputBuffer = new ArrayList<Tuple>();
			
			while(bwOutDirSize != outDirSize && !usedRuns.isEmpty()) { //merging 1 step (stop merge when outDirSize number of 
				//runs in output temp directory)
				//find the minTup among the tuples added
//				System.out.println(bwOutDirSize);
				System.out.println(usedRuns.size());
				Tuple minTup = null;
				CompareTuples tc = new CompareTuples();
				int minTupIndx = 0;
				int inc = 0;
				
				
				
				//finding the min tuple
				for(Tuple tup : bMinusOneTuple) {
					if (tup == null) {
						break;
					}
					if (minTup == null){
						minTup = tup;
						minTupIndx = inc;
					} else if (tc.compare(tup,minTup) == -1){
						minTup = tup;
						minTupIndx = inc;
					}
					
					inc++;
				}
				//my file reader adds up to bMinusOneTuple
				//add min to output buffer and "remove" it from input buffer
				
				
				
				
				
				//writing to output buffer
				outputBuffer.add(minTup);
				outBufferNumTup++;
				//updating the input buffer... might have indexing errors...
				
				
				
				
				
				//updating the input buffer
				if (fileReaders.get(minTupIndx).nextTuple() == null) {//if there exists unusedRun to be used
					if(!unusedRuns.isEmpty()) {
						usedRuns.set(minTupIndx, unusedRuns.get(0));
						unusedRuns.remove(0);
					} else {
						usedRuns.remove(minTupIndx);
						bMinusOneTuple.remove(minTupIndx);
					}
				} else{ //else just keep getting next tuple from the same run
					bMinusOneTuple.set(minTupIndx, fileReaders.get(minTupIndx).nextTuple());
				}
				
				
				
				
				//write to disk and clear output buffer
				//checking the output buffer and writing to "disk"
				if (outBufferNumTup == tuplesPerPage || usedRuns.isEmpty()) { //do we have to create files whenever the 
					//output buffer is full? slide says to write to disk when output buffer is full
					//but I want each files to be intermediate runs, two runs combined, but if i 
					//output whenever output buffer is full, the files will only contain one page, not 
					//runs combined...
					System.out.println("INSIDEE");
					String fileName = tempDir + "mergeStep_" + Integer.toString(ms) + "_run_" + Integer.toString(rn);
					fileList.set(bwOutDirSize, fileName); //overwrite fileList and get first n elements next merge
					bwOutDirSize ++;
					ms++;
					rn++;
					BinaryTupleWriter writer = new BinaryTupleWriter(fileName);
					System.out.println("outbuff size: " + outputBuffer.size());
					for(Tuple t : outputBuffer) {
						if (t == null) {
							break;
						}
					    writer.writeTuple(t);
					} //does this pad the binary file with zeros?
					writer.close();
					FileConverter.convertBinToHuman(fileName, fileName + "_humanreadable");
					outputBuffer = new ArrayList<Tuple>();//reset output buffer
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
