package p1.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import p1.index.BTree;
import p1.index.BTreeLeafNode;
import p1.index.TupleIdentifier;
import p1.io.BPTreeReader;
import p1.io.BinaryTupleWriter;
import p1.util.DatabaseCatalog;
import p1.util.Tuple;

/**
 * An operator that opens a file scan on the appropriate data file to return all
 * rows in that table.
 */
public class IndexScanOperator extends ScanOperator {
	// Column names.
	ArrayList<String> schema;
	// Table name.
	String table;
	private Integer highkey;
	private Integer lowkey;
	private String indexFile;
	private Boolean isClustered;
	private int colIdx; // index of the attribute column.
	private TupleIdentifier currRid;
	private int currKey;
	private int currTuple; // curr tuple within key
	// index file reader
	BPTreeReader reader;
	ArrayList<ArrayList<Integer>> rids = new ArrayList<ArrayList<Integer>>();
	int keyPos;
	int child;
	ArrayList<Integer> keys = new ArrayList<Integer>();
	private Integer currPageID;
	private Integer currTupleID;
	private ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> leaf;
	private ArrayList<ArrayList<Integer>> currPos;
	private Object[] valueSet;
	private Object[] valueArray;
	private List<Object> valueList;
	private ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> templeaf;
	private ArrayList<Entry<Integer, ArrayList<TupleIdentifier>>> currLeafNode;
	private Entry<Integer, ArrayList<TupleIdentifier>> currRow;
	private int currPage;

	/**
	 * Constructor to scan rows of table fromTable using indexes.
	 */
	public IndexScanOperator(String fromTable, Integer lowkey, Integer highkey, Boolean clustered, int colIdx,
			String indexFile) {
		
		super(fromTable);
		
		this.highkey = highkey;
		this.lowkey = lowkey;
		this.isClustered = clustered;
		this.indexFile = indexFile;
		this.colIdx = colIdx; 
		
		reader = new BPTreeReader(indexFile); 
		reader.checkNodeType();
		int rootAddy = reader.getAddressOfRoot(); 
		
		currKey = 0; 
		currTuple = 0; 
		keyPos = 0;
		
		if (lowkey == null) { 			
			reader.reset(1); 

			reader.checkNodeType(); 
			
			for (Integer i : reader.getNextDataEntryUnclus().keySet()) { 
				currKey = i;
			}
			
		} else { 
			for (int i = 0; i < rootAddy; i++) { // WORKS
				reader.checkNodeType(); 
			} 			
			currKey = reader.getNextKey(); 
						
			reader.reset(rootAddy);
			while (!(reader.checkNodeType())) {
				currKey = reader.getNextKey();
				keys.clear();
				int temp = 0;
				int pos = 0;;
				keys.add(currKey);
				Boolean found = false;
				while ((currKey) != -1) {
					keys.add(currKey);
					temp++;
					if (lowkey < currKey && !found) {pos = temp; found = true;}
					currKey = reader.getNextKey();
				} 
				if (!found) {
					pos = temp;
				}					
			currKey = reader.getNextKey();
			
			for (int i = 0; i < pos; i++) {
				child = reader.getNextAddrIN(); 
			}
				
			currKey = child;
			reader.reset(currKey);	
			
			}
			
			keyPos = -1;
			if (currKey < lowkey) {
				while (currKey < lowkey) {
					for (Integer i : reader.getNextDataEntryUnclus().keySet()) currKey = i; keyPos++;
					if (currKey == lowkey) currKey = lowkey;
				}
			} else {
				currKey = lowkey;
			}
		} 
				
//		getNextTuple();	
//		System.out.println("---------------------------");
		}
	
	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
//		System.out.println("avav;abjdvbadjb");
		Tuple tuple = null;
		
		while (true) {
			if (isClustered) { 
				tuple = super.getNextTuple();

				if (tuple == null) {
					return null;
				} 
				if (highkey != null && currRow.getKey() > highkey) {
					System.out.println("__________");
					return null;
				} 
			} 
			
			else { //unclustered 
//				System.out.println("Entered thsi loop");
				currLeafNode = reader.deserializeLeafNode().getReference();				
//				System.out.println("currLeafNode: " + currLeafNode);				
//				System.out.println("keyPos: " + keyPos);
//				System.out.println("currTuple: " + currTuple);
//				
				currRow = currLeafNode.get(keyPos);				
//				System.out.println("currRow: " + currRow);
																																	
				if (keyPos >= currLeafNode.size() - 1) { //read all keys on page 
//					System.out.println("read all keys on page ");
					if(reader.checkNodeType() == true) {
//						System.out.println("going to next page");
						keyPos = 0; 
						currTuple = 0; 
					} else {
						return null;
					}
//					nextPage = reader.checkNodeType();
//					if (reader.checkNodeType() == false) return null; //finished traversing all leaves
					
					
//					reader.checkNodeType(); // go to next page 
					
				} 				
				
				//reached highkey 
				if (highkey != null && currRow.getKey() >= highkey) {
//					System.out.println("reached highkey");
					return null;
				} 
								
//				System.out.println("tuples in row: " + currRow.getValue().size());
//				System.out.println("currTuple2: " + currTuple);

				if (currTuple >= currRow.getValue().size() - 1) { //read all tuples in row
//					System.out.println("read all tuples in key");

                    keyPos++;
                    currTuple = 0;
                }
				
				currRid = currRow.getValue().get(currTuple);
//				System.out.println("currRid: " + currRid);	
			
				currPageID = currRid.getPageId();
//				System.out.println("currPageID: " + currPageID);

				currTupleID = currRid.getTupleId();
//				System.out.println("currTupleID: " + currTupleID);
				
//				System.out.println("currTuple: " + currTuple);

//				System.out.println("calling next tuple");
				try {
					tuple = super.getNextTupleIndex(currRid, currPageID, currTupleID);
//					System.out.println(tuple);
					currTuple++;		

				} catch (IOException e) {
					e.printStackTrace();
				}
//				System.out.println("TUPLE: " + tuple);
								
//				System.out.println("done");

			}	
			
			return tuple;
		}
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		super.reset();
	}

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the scan table.
	 */
	public ArrayList<String> getSchema() {
		return super.getSchema();
	}

	/**
	 * Gets the table name.
	 *
	 * @return the table name.
	 */
	public String getTable() {
		return table;
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	public void dump() {
		super.dump();

	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	public void dump(String outputFile) {
		Tuple nextTuple = getNextTuple();
		while (nextTuple != null) {
			try {
				BinaryTupleWriter out = new BinaryTupleWriter(outputFile);
				while (nextTuple != null) {
					out.writeTuple(nextTuple);
					nextTuple = getNextTuple();
				}
				out.close();
			} catch (Exception e) {
				System.out.println("Exception occurred: ");
				e.printStackTrace();
			}
			
		}
	}

}
