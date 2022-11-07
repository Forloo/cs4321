package p1.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import p1.index.BTree;
import p1.index.BTreeLeafNode;
import p1.io.BPTreeReader;
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
	private ArrayList<Integer> currRid;
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
			if (currKey < lowkey) {
				while (currKey < lowkey) {
					for (Integer i : reader.getNextDataEntryUnclus().keySet()) currKey = i; keyPos++;
					if (currKey == lowkey) currKey = lowkey;
				}
			} else {
				currKey = lowkey;
			}
		} 
		System.out.println(currKey);
		System.out.println(reader.getNextDataEntryUnclus());
																		
		System.out.println("---------------------------");
		}
	
	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		Tuple tuple = null;

		while (true) {
			if (isClustered) { 
				tuple = super.getNextTuple();
				if (tuple == null) {
					return null;
				} 
				if (highkey != null && currKey > highkey) {
					return null;
				} 
			} 
			
			else { //unclustered 
				reader.resetIdx();
				leaf = reader.deserializeLeaf();
				System.out.println("leaf: " + leaf);

				
				currRid = leaf.get(keyPos).get(currKey).get(currTuple);

				System.out.println("leaf: " + leaf);

				System.out.println("keyPos: " + keyPos);
				System.out.println("key: " + leaf.get(keyPos).get(5).get(currTuple));
				System.out.println("currTuple: " + currTuple);
																		
																
				if (keyPos > leaf.size()) { //read all keys on page 
					System.out.println("2");

					if (reader.checkNodeType() == false) return null; //finished traversing all leaves
					reader.checkNodeType(); // go to next page 
					keyPos = 0; 
					currTuple = 0; 
					continue;
				} 				
								
				//reached highkey 
				if (highkey != null && currKey >= highkey) return null;
		
				if (currTuple >= rids.size()) { //read all tuples in key
					System.out.println("3");

                    keyPos++;
                    currTuple = 0;
                    continue;
                }
			
				System.out.println("4");

								
				currRid = rids.get(currTuple);
				currPageID = rids.get(currTuple).get(0);
				currTupleID = rids.get(currTuple).get(1);

				try {
					System.out.println("5");

					tuple = super.getNextTupleIndex(currRid, currPageID, currTupleID);
					currTuple++;
				} catch (IOException e) {
					e.printStackTrace();					
				}				
			}					
			System.out.println("6");

			System.out.println("TUPLE: " + tuple);
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
		super.dump(outputFile);
	}

}
