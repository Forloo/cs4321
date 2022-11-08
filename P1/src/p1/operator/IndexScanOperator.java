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
	private Boolean done = false;

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
		this.table = fromTable;
		
		System.out.println(table);

		
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
//		getNextTuple();	
//		getNextTuple();	
//
//		getNextTuple();	


//		System.out.println("---------------------------");
		}
	
	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		Tuple tuple = null;	
		
		if (isClustered) { 

			
			tuple = super.getNextTuple();


			if (tuple == null) {
				return null;
			} 
			if (highkey != null && currRow.getKey() > highkey) {
				System.out.println("__________");
				return null;
			} 
			return tuple;
		} 
		
		else { //unclustered 
			if (!done) {
				//get the entire leaf
				currLeafNode = reader.deserializeLeafNode().getReference();
				//get to the correct row
				currRow = currLeafNode.get(keyPos);
				//get the page id and tuple id and get the tuple
				currRid = currRow.getValue().get(currTuple);
				currPageID = currRid.getPageId();
				currTupleID = currRid.getTupleId();
				try {
					tuple = super.getNextTupleIndex(currRid, currPageID, currTupleID);
//					System.out.println(tuple);
//					System.out.println("increment the tuple count");
					currTuple++;
				} catch (IOException e) {
					e.printStackTrace();
				}
//					System.out.println("TUPLE: " + tuple);
//					System.out.println("done");
				
				//check if we read all the tuples in the row
				if (currTuple > currRow.getValue().size() - 1) { //read all tuples in row
//					System.out.println("finished reading all tuples in the row, move on to next row");
                    keyPos++;
                    currTuple = 0;
                }
				
				
				//check if you have read all the keys in the page
				if (keyPos > currLeafNode.size() - 1) { //read all keys on page
					//if you read all the keys in the page and next page is index, return null
					if (reader.checkNodeType() == false) {
						done = true;
						return tuple;
					} 
					//if not, go to the next page and reset the variables.
					keyPos = 0; 
					currTuple = 0;
				}
				
				//check if we reached the highkey
				if (highkey != null && currRow.getKey() > highkey) {
					done = true;
					return tuple;
				} 
				
				
				if (tuple != null) {
					return tuple;
				
			} else {
				return null;
			}
				}
			System.out.println(tuple);

			
		}
		return null;	
			
		
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
		Tuple next = this.getNextTuple();
		while (next != null) {
			System.out.println(next.toString());
		}

	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	public void dump(String outputFile) {
		Tuple nextTuple = this.getNextTuple();
		while (nextTuple != null) {
			try {
				BinaryTupleWriter out = new BinaryTupleWriter(outputFile);
				while (nextTuple != null) {
					out.writeTuple(nextTuple);
					nextTuple = getNextTuple();
				}
				reader.close();
			} catch (Exception e) {
				System.out.println("Exception occurred: ");
				e.printStackTrace();
			}
			
		} 		reader.close();

	}

}
