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
		System.out.println("\nnew table\n");
		System.out.println((int)2.6);
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
		int numRunsAfterMerge = fileList.size() * 2;//issue is when we start with n number of runs where n is not a power of 2
		tuplesPerPage = tuplesPerPage * b;//initialize output buffer size or run size of merge step
		int ms = 0; //merge step for storing files in temp dir
		int rn = 0; //n'th run for storing files in temp dir
		
		while(numRunsAfterMerge != 1) { //for merging into one big run
			numRunsAfterMerge = numRunsAfterMerge / 2; //same issue as mentioned
			
			if (numRunsAfterMerge == 1) {//included here bc once putting under while loop below, code doesn't reach
				break;
			}
			
			//adjust fileList here, potential issue here
			List<String> subItems = new ArrayList<String>(fileList.subList(0, numRunsAfterMerge)); //change
			
			
			//initialize all runs
			ArrayList<BinaryTupleReader> fileReaders = new ArrayList<BinaryTupleReader>();
			for (String f : subItems) {
				//make BinaryTupleReader for all runs...
				fileReaders.add(new BinaryTupleReader(f));
			}
			
			//initialize input buffer (inputBufferRun) (load the tuples)
			ArrayList<BinaryTupleReader> inputBufferRun = new ArrayList<BinaryTupleReader>();
			ArrayList<Tuple> inputBufferTuple = new ArrayList<Tuple>();
			int numTupInBuff = 0;
			for (BinaryTupleReader fileRead : fileReaders) {
				if(numTupInBuff<b-1) {
					inputBufferTuple.add(fileRead.nextTuple());
					numTupInBuff++;
					inputBufferRun.add(fileRead);
				} else {
					break;
				}
				
			}
			
			//create uninputBufferRun (leftover runs to refill input buffer once one runs out)
			ArrayList<BinaryTupleReader> uninputBufferRun = new ArrayList<BinaryTupleReader>();
			int copy = numTupInBuff;
			if (copy == b-1) { //when equal amount of tuples in input buff, there might be left over runs...
				try {
					while(copy<=fileReaders.size()-1) {
						uninputBufferRun.add(fileReaders.get(copy));
						copy++;
					}
				} catch (Exception e){
					continue;
				}
				
			}
			
			
			
			int outBufferNumTup = 0; //to check how many tuples in output buffer now
			int currentNumRuns = 0; //to check current merge step's number of produced runs
			tuplesPerPage = tuplesPerPage * 2;
			
			
			//for debugging (check if initialized properly)
			for (String title:fileList) {
				System.out.println(title);//print fileList for debugging
			}
			System.out.println("fileReader Size: "+fileReaders.size()); //all runs
			System.out.println("bminus one tuple size: " + inputBufferTuple.size()); //tuples
			System.out.println("used run size: "  +inputBufferRun.size()); //input buff runs
			System.out.println("unused run size: "  +uninputBufferRun.size()); //potential input buff runs
			System.out.println("out dir number of tuples should be: "+ tuplesPerPage); //this merge step's run size
			System.out.println("out dir size: " + numRunsAfterMerge);
			
			
			
			ArrayList<Tuple> outputBuffer = new ArrayList<Tuple>(); //intialize output buffer
			ms++; //for storing files, creating unique names
			rn = 0;
			while(!inputBufferTuple.isEmpty()) { //1 merge step
				
				//finding min tuple, saving which input buffer page min came from
				Tuple minTup = null;
				CompareTuples tc = new CompareTuples();
				int minTupIndx = 0;
				int inc = 0;
				for(Tuple tup : inputBufferTuple) {
					if (minTup == null){
						minTup = tup;
						minTupIndx = inc;
					} else if (tc.compare(tup,minTup) == -1){
						minTup = tup;
						minTupIndx = inc;
					}
					
					inc++;
				}

				
				
				
				//writing to output buffer
				outputBuffer.add(minTup);
				outBufferNumTup++;
//				System.out.println(outBufferNumTup);
				
				
				
				
				Tuple nextTupInBuff = inputBufferRun.get(minTupIndx).nextTuple();
				//updating the input buffer
				if (nextTupInBuff == null) {//if there exists unusedRun to be used
					if(!uninputBufferRun.isEmpty()) {
						System.out.println("refill the input buffer");
						inputBufferRun.set(minTupIndx, uninputBufferRun.get(0));
						Tuple nextTup = inputBufferRun.get(minTupIndx).nextTuple();
//						System.out.println("size of unused input buffer: " + uninputBufferRun.size());
						inputBufferTuple.set(minTupIndx, nextTup);
						uninputBufferRun.remove(0);
					} else {
						inputBufferRun.remove(minTupIndx);
						inputBufferTuple.remove(minTupIndx);
						System.out.println("input buffer tuple array size is: "+inputBufferTuple.size());
					}
				} else{ //else just keep getting next tuple from the same run
//					System.out.println("called?");
					inputBufferTuple.set(minTupIndx, nextTupInBuff);
				}

				
				
				
				//write the disk and clear output buffer
				if (outBufferNumTup == tuplesPerPage || inputBufferTuple.isEmpty()) {
					String fileName = tempDir + "mergeStep_" + Integer.toString(ms) + "_run_" + Integer.toString(rn);
//					fileList.set(currentNumRuns, fileName); //overwrite fileList and get first n elements next merge???
					fileList.add(0,fileName); //try this?
					currentNumRuns ++; //for while loop end condition
					
					rn++; //creating unique names
					
					//write to temp dir
					BinaryTupleWriter writer = new BinaryTupleWriter(fileName);
					System.out.println("this many tuples stored in output buffer: " + outputBuffer.size());
					System.out.println("writing this file to temp dir: " + fileName);
//					outputBuffer.add(nextTupInBuff)
					for(Tuple t : outputBuffer) {
						if (t == null) {
							break;
						}
					    writer.writeTuple(t);
					} //does this pad the binary file with zeros?
					writer.close();
					FileConverter.convertBinToHuman(fileName, fileName + "_humanreadable"); //for debugging
					outputBuffer = new ArrayList<Tuple>();//reset output buffer
					outBufferNumTup = 0;//reset output buffer (for efficiency)
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
