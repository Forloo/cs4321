package p1.operator;

import java.io.IOException;
import java.util.ArrayList;

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
	private BTree btree;
	private BTreeLeafNode currLeafNode;
	private String indexFile;
	private Boolean isClustered;
	private int colIdx; // index of the attribute column.
	private ArrayList<Integer> currRid;
	private int currKey;
	private int currTuple; // curr tuple within key
	// index file reader
	BPTreeReader reader;
	private int idx;
	private int currPage;
	ArrayList<ArrayList<Integer>> rids = new ArrayList<ArrayList<Integer>>();
	int keyPos;
	ArrayList<ArrayList<ArrayList<Integer>>> ridList;
	int child;
	ArrayList<Integer> keys = new ArrayList<Integer>();

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

		int order = reader.getOrderOfTree(); 

		int rootAddy = reader.getAddressOfRoot(); 

		int numLeaves = reader.getNumLeaves(); 
		
		currKey = 0; 
		currTuple = 0; 
		
		keyPos = 0;
		
		//find lowkey first 

		if (lowkey == null) { 
			reader.reset(1); //start at first page if no lower bound 
//			reader.checkNodeType();
			currKey = reader.getNextKey(); // smallest leaf node 
			System.out.println("currKey: " + currKey); 
			System.out.println(reader.checkNodeType()); 
			System.out.println(reader.getNextDataEntryUnclus().get(currKey)); // smallest leaf node 
			System.out.println(reader.getNextDataEntryUnclus().get(reader.getNextKey())); // smallest leaf node 
		
		} else { 
			for (int i = 0; i < rootAddy; i++) { 
				reader.checkNodeType(); // get to root node	
			} 			
			currKey = reader.getNextKey();
		
			System.out.println("first key in root: " + currKey);
			
			System.out.println("lowkey: " + lowkey);
					
			if (lowkey < currKey) {			
				while (lowkey <= currKey) { 
					while (currKey != -1) {
						currKey = reader.getNextKey();
						System.out.println("currKey: " + currKey); 	
						keys.add(currKey);
					} 
	
					currKey = reader.getNextKey();
					child = reader.getNextAddrIN(); // first child address
					System.out.println("child: " + child); 
					
					if (lowkey <= child) { 
						reader.reset(child);
						reader.checkNodeType(); //goes to next index page 
						currKey = reader.getNextKey(); 
						System.out.println("currKey in loop: " + currKey); 
					}		
					
					reader.reset(child);
					System.out.println(reader.checkNodeType()); 
					currKey = reader.getNextKey(); 
					System.out.println("currKey: " + currKey); 				
				} 
				
			} else if (lowkey >= currKey) {
				while (lowkey > currKey) {
				int temp = 0;
				int pos = 0;;
				keys.add(currKey);
				while (currKey != -1) {
					temp++;
					if (lowkey >= currKey) pos = temp;
					currKey = reader.getNextKey();
					if (currKey != -1) keys.add(currKey);
					System.out.println(keys);					
				} 
				System.out.println("pos" + pos);
								
				currKey = reader.getNextKey();
							
				for (int i = 0; i < pos + 1; i++) {
						child = reader.getNextAddrIN(); 
						System.out.println("child: " + child);
					}
				
				}
				currKey = child;
				System.out.println("reset to: " + currKey); 				

				
				reader.reset(currKey);
				reader.checkNodeType();
				currKey = reader.getNextKey();
				System.out.println("currKey: " + currKey); 	
			
				
			}
			}
		
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
				if (highkey != null && Integer.valueOf(tuple.getTuple().get(colIdx)) > highkey) {
					return null;
				}
				return tuple;
			} 
			
			else { //unclustered 
				if (currKey >= reader.getNumKeys()) { //read next page 
					if (reader.checkNodeType() == false) return null; //finished traversing all leaves
					keyPos = 0; // start reading first key
					currTuple = 0; // start from first tuple 
					reader.getNextKey(); 
				} 
				
				//reached upper bound
				if (highkey != null && currKey > highkey) return null;
				
				
				rids = reader.getNextDataEntryUnclus().get(currKey); // list of rids for currKey 
				System.out.println("rids: " + rids);


				if (currTuple >= keys.size()) { // read all tuples for currKey
					keyPos++;
					currTuple = 0; // start reading from first tuple on next page
				} 
								
				for (int i = 0; i < keys.size(); i++) {
					currRid = rids.get(i);
					int currPageID = rids.get(i).get(0);
					int currTupleID = rids.get(i).get(1);
					System.out.println("currTuple: " + currTuple);

					try {
						super.getNextTupleIndex(currRid, currPageID, currTupleID);
						currTuple++;

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				currKey = reader.getNextKey();
				
			}
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
	 * Resets the Operator to the ith tuple.
	 *
	 * @param idx the index to reset the Operator to
	 */
	public void reset(int idx) {
	}

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the scan table.
	 */
	public ArrayList<String> getSchema() {
		return schema;
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
