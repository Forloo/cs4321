package p1.operator;

import java.io.File;
import java.util.ArrayList;

import p1.index.BTree;
import p1.index.BTreeLeafNode;
import p1.io.BinaryTupleReader;
import p1.util.DatabaseCatalog;
import p1.util.Tuple;
import p1.io.BPTreeReader;

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
	private File indexFile;
	private Boolean isClustered;
	private int colIdx;	// index of the attribute column.
	private int currRid;
	private int currKey;
	private int currTuple; // curr tuple within key 
	//index file reader
	BPTreeReader reader;
	private int idx;

	/**
	 * Constructor to scan rows of table fromTable using indexes.
	 */
	public IndexScanOperator(String fromTable, Integer lowkey, Integer highkey, Boolean clustered, int colIdx, File indexFile) {
		super(fromTable);
		this.highkey = highkey;
		this.lowkey = lowkey;
		this.isClustered = clustered;
		this.indexFile = indexFile;
		this.colIdx = colIdx;
		reader = new BPTreeReader(indexFile.toString());
		
		if (lowkey == null) {
			currLeafNode = null; // smallest leaf node 
		} else {
			// start at lowkey --> while currKey < lowkey currKey ++
//			currKey = reader.getNextKey();
		}
		
		
		currTuple = 0;
				
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
				if (highkey != null && colIdx > highkey) {
					return null;	
				}
				return tuple;
			}
			else { //unclustered 
				if (currKey >= reader.getNumKeys()) {
					if (reader.getNextAddrIN() + 1 > reader.getNumLeaves()) { //read all leaves already 
						return null;
					}
					for (int i = 0; i < reader.getNumKeys(); i++) {
						for (int j = 0; j < reader.getNumLeaves(); j ++) {
//							currLeafNode = (BTreeLeafNode) reader.getNextDataEntryUnclus().get();
							// help
						}
					}
					reader.getNextKey();
					currKey++;
					
				}
				if (tuple != null) return tuple;
				
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
