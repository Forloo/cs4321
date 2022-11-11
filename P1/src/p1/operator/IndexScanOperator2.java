package p1.operator;

import java.util.ArrayList;
import java.util.Map;

import p1.index.BTreeIndexNode;
import p1.index.BTreeLeafNode;
import p1.index.BTreeNode;
import p1.index.TupleIdentifier;
import p1.io.BPTreeReader;
import p1.io.BinaryTupleWriter;
import p1.util.Tuple;

public class IndexScanOperator2 extends ScanOperator {

	// The lowkey for the scan Index
	private Integer lowKey;
	// The highKey for the scan index
	private Integer highKey;
	// whether the index that we are scanning from is clustered or not
	private boolean clustered;
	// The column index that the table is indexed on
	private int colIdx;
	// The reader for the binary tree file
	private BPTreeReader treeReader;
	// The current page the BPTreeReader is on for the leaf node
	private int currPage;
	// The current reference id location for the current key
	private int currRid;
	// The current key that we are on for the current leaf node
	private Integer currKey;
	// The current leaf node for the page that the current bufffer is on.
	private BTreeLeafNode currNode;
	// The number of leaf nodes in the tree
	private int numLeafs;
	// The location of the root node
	private int rootAddy;
	// The order for the tree that we are traversing
	private int order;
	// Page for the leaf node the query starts on.
	private int startLeafPage;
	// The index of the key the leaf page starts on.
	private Integer startKey;

	public IndexScanOperator2(String fromTable, Integer lowKey, Integer highKey, Boolean clustered, int colIdx,
			String indexFile) {
		super(fromTable);
		this.lowKey = lowKey;
		this.highKey = highKey;
		this.clustered = clustered;
		this.colIdx = colIdx;
		this.treeReader = new BPTreeReader(indexFile);

		// Read the header page and get the info from that page
		this.treeReader.resetBuffer();
		this.treeReader.read(0);

		ArrayList<Integer> headerInfo = this.treeReader.getHeaderInfo();
		for (int i = 0; i < headerInfo.size(); i++) {
			if (i == 0) {
				this.rootAddy = headerInfo.get(i);
			} else if (i == 1) {
				this.numLeafs = headerInfo.get(i);
			} else {
				this.order = headerInfo.get(i);
			}
		}

		// Initialize this value to null first.
		currKey = null;

		// If the lowkey value here is null then that means that we want to get the
		// first key value that
		// is in the left most leaf node.
		if (this.lowKey == null) {
			// The first leaf node will be on the first page for this
			this.treeReader.resetBuffer();
			this.treeReader.read(1);
			// We know that this node must be a leaf node since this is the first element
			// in the root page
			currNode = (BTreeLeafNode) this.treeReader.deserializeNode();

			// Set the curr page that we are reading from to be the page one
			currPage = 1;
			// The rid for the current key always start on the
			currRid = 0;
			// The curr leaf node that we are on for the current leaf node
			currKey = 0;

			// The leaf page starts on the first page in the file
			startLeafPage = 1;
			startKey = 0;
		} else {
			// Start at the root node and then traverse to the leaf node
			this.treeReader.resetBuffer();
			this.treeReader.read(this.rootAddy);

			BTreeNode temp = this.treeReader.deserializeNode();
			int address = 0;

			while (!(temp instanceof BTreeLeafNode)) {
				// If we are in this loop then that means the node is an index node meaning that
				// we can cast to it
				BTreeIndexNode changed = (BTreeIndexNode) temp;
				ArrayList<Map.Entry<Integer, ArrayList<Integer>>> pointers = changed.getReferences();

				// Iterate through the arraylist until we find a pointer that is either lest
				// than or equal
				// to the current lowkey value
				for (int i = 0; i < pointers.size(); i++) {
					// Check if we are on the last key since this is the one with two values in its
					// pointers
					if (i == pointers.size() - 1) {
						if (this.lowKey < pointers.get(i).getKey()) {
							address = pointers.get(i).getValue().get(0);
						} else {
							address = pointers.get(i).getValue().get(1);
						}
					} else if (this.lowKey < pointers.get(i).getKey()) {
						address = pointers.get(i).getValue().get(0);
						break;
					}
				}

				// After you get the address to jump to then we need to update the current
				// btreenode
				this.treeReader.resetBuffer();
				this.treeReader.read(address);

				temp = this.treeReader.deserializeNode();
			}

			ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> findKeyStart = temp.getReference();

			// Get to the right key in the given node if there is a key for it.
			for (int k = 0; k < findKeyStart.size(); k++) {
				int keyValue = findKeyStart.get(k).getKey();
				if (keyValue >= this.lowKey) {
					currKey = k;
					break;
				}
			}

			// Set the right node for this or if index node then not possible
			if (currKey == null) {
				this.treeReader.resetBuffer();
				address = address + 1;
				this.treeReader.read(address);
				temp = this.treeReader.deserializeNode();
				if (temp instanceof BTreeIndexNode) {
					currKey = null;
					currRid = 0;
					currNode = null;
				} else {
					currKey = 0;
					currRid = 0;
					currNode = (BTreeLeafNode) temp;
				}
			} else {
				currRid = 0;
				currNode = (BTreeLeafNode) temp;
			}

			// Need this to reset to the right leaf page and to the right key later
			startLeafPage = address;
			startKey = currKey;
			currPage = address;

		}

	}

	public Tuple getNextTuple() {
		// List of things to check
		// 1. If the currNode is none then return the value of null
		// 2 actual one. Alternative check is to see if the key value that we are
		// checking has a value that
		// is larger than the highkey
		// 2.Need to check if the current key for the node that we are checking if that
		// is still in bound
		// if that value is in bound then move to next rid number. If the number if not
		// in bound then move
		// to the next key value. If the next key value will get us out of range then we
		// need to read the next
		// possible leaf page. If the next page that we read is not a leaf node then
		// make the currNode null

		if (currNode == null) {
//			System.out.println("The curr node ended up being node");
			return null;
		}
		if (this.highKey != null && currNode.getReference().get(currKey).getKey() > this.highKey) {
//			System.out.println("The value of the key is larger than the highkey value that we are given");
			return null;
		}
		// Get the tuple value here
		ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> info = currNode.getReference();
		int page = info.get(currKey).getValue().get(currRid).getPageId();
		int tupleId = info.get(currKey).getValue().get(currRid).getTupleId();
		Tuple result = super.getNextTupleIndexScan(page, tupleId);

		// If the currrid location is still lower then the length of the currValue row
		// then that
		// means that we are still in bound
		if (currRid + 1 < currNode.getReference().get(currKey).getValue().size()) {
			currRid = currRid + 1;
		}
		// Move to the next key value
		else {
			if (currKey + 1 < currNode.getReference().size()) {
				currKey = currKey + 1;
				// Need to reset the currRid back to the zero value
				currRid = 0;
			}
			// If there are no more keys in this node then that means we are done with this
			// node and that
			// we need to get the next leaf node
			else {
				this.treeReader.resetBuffer();
				// Increment the current page by the value one to get the next leaf page
				currPage = currPage + 1;
				this.treeReader.read(currPage);
				BTreeNode node = this.treeReader.deserializeNode();
				if (node instanceof BTreeLeafNode) {
					currNode = (BTreeLeafNode) node;
					// Set the current currentKey value to have the value zero
					currKey = 0;
					// Set the current rid value of that key to also be the value of zero.
					currRid = 0;
				} else {
					currNode = null;
				}
			}

		}

		return result;
	}

	public void reset() {
		// To reset the leaf node there are three things that we need to do
		// 1. Move to the right buffer page
		// 2. Read the values from the buffer page correctly
		// Update the currNode to be the currNode-> If the page that we are given is not
		// a leaf address then that
		// that means the node is null and then there is nothing for us to check

		this.treeReader.resetBuffer();
		this.treeReader.read(startLeafPage);

		// Make the node
		BTreeNode node = this.treeReader.deserializeNode();

		if (node instanceof BTreeIndexNode) {
			currNode = null;
		} else {
			// This means that the page is a leaf node meaning that there are some values
			// that we can get from it
			currNode = (BTreeLeafNode) node;

			// The currid that we start on will always be the value of zero
			currRid = 0;
			// The key that we start on will have a different value
			currPage = startLeafPage;
			// The key that we start on for that given page
			currKey = startKey;
		}
	}

	public ArrayList<String> getSchema() {
		return super.getSchema();
	}

	public String getTable() {
		return super.getTable();
	}

	public void dump() {
		Tuple nextTuple = getNextTuple();
		while (nextTuple != null) {
			System.out.println(nextTuple.toString());
			nextTuple = getNextTuple();
		}
	}

	public void dump(String outputFile) {
		Tuple nextTuple = getNextTuple();
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
