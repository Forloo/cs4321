package p1.index;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import p1.io.BinaryTupleReader;
import p1.io.FileConverter;
import p1.operator.ExternalSortOperator;
import p1.operator.ScanOperator;
import p1.util.DatabaseCatalog;
import p1.util.Tuple;

public class BTree {

	// Min entries per order. Max entries is 2*order.
	private int order;
	// The root index node
	private BTreeNode root;
	// Leaf node is clustered or not clustered
	private boolean clustered;
	// The page size for all nodes.
	private final static int pageSize = 4096;
	// The column the index is constructed based on
	private int indexedColumn;
	// The output file that we write the final index to
	private File indexFile;
	// The relation that we are reading from
	private String inputTable;
	// A reader containing all of the tuples for the given file
	private BinaryTupleReader reader;
	// The next address for the next node
	private int pageTracker;
	// Each level of the tree structure represented as an arraylist of nodes.
	private ArrayList<ArrayList<BTreeNode>> allNodeLevels;

	/**
	 * The constructor for the B+ tree
	 * 
	 * @param order:         Min number of elements in each node of the tree. Max is
	 *                       2*order
	 * @param clustered:     Index is clustered or not
	 * @param indexedColumn: The column that we are indexing on
	 * @param indexFile:     The output file that we will be writing our index
	 *                       information to.
	 */
	public BTree(int order, boolean clustered, int indexedColumn, File indexFile, String input, int pageTracker) {
		this.order = order;
		this.root = null;
		this.clustered = clustered;
		this.indexedColumn = indexedColumn;
		this.indexFile = indexFile;
		this.inputTable = input;
		this.pageTracker = pageTracker;
		this.allNodeLevels = new ArrayList<ArrayList<BTreeNode>>();

		// If clustered then sort the relation table
		if (!this.clustered) {
			// Do some sorting on the relation beforehand.
//			ScanOperator scan = new ScanOperator(input);
			BinaryTupleReader reader = new BinaryTupleReader(input);
			this.reader = reader;
		} else {
			// Reading from the input file to get all of the binary tuples
			// Make the scan operator in here for now
			ScanOperator scan = new ScanOperator(input);
//			System.out.println(scan.getNextTuple());
			// Get the schema so after that I can pass in the ordering that I want for the
			// external sort operation
			ArrayList<String> ordering = scan.getSchema();
			ArrayList<String> newOrdering = new ArrayList<String>();
			// Make a deep copy of the schema
			for (String element : ordering) {
				String curr = element;
				newOrdering.add(element);
			}

			if (indexedColumn != 0) {
				Collections.swap(newOrdering, 0, indexedColumn);
			}

			ExternalSortOperator external = new ExternalSortOperator(scan, newOrdering,
					DatabaseCatalog.getInstance().getSortPages(), DatabaseCatalog.getInstance().getTempDir(), 0);

			external.dump(DatabaseCatalog.getInstance().getNames().get(input));
			String humanPath = DatabaseCatalog.getInstance().getNames().get(input) + "_humanreadable";
			FileConverter.convertBinToHuman(DatabaseCatalog.getInstance().getNames().get(input), humanPath);
			BinaryTupleReader reader = new BinaryTupleReader(DatabaseCatalog.getInstance().getNames().get(input));
//			this.reader=reader;
			this.reader = reader;
		}
	}

	/**
	 * Set the root node of the B+ tree
	 * 
	 * @param rootNode
	 */
	public void setRoot(BTreeNode rootNode) {
		this.root = rootNode;
	}

	/**
	 * Get the root node of the B+ tree
	 * 
	 * @return root node of B+ tree
	 */
	public BTreeNode getRoot() {
		return root;
	}

	/**
	 * Retrieves all levels of the B+ tree. The 0th level is the leaf index level
	 * 
	 * @return
	 */
	public ArrayList<ArrayList<BTreeNode>> getAllLevels() {
		return allNodeLevels;
	}

	/**
	 * Returns the next address node for the next node to be made.
	 * 
	 * @return An integer representing the address for the next node
	 */
	public int getPageTracker() {
		return this.pageTracker;
	}

	/**
	 * Constructs the index tree
	 * 
	 * @return A BTreeNode that will be the root for the index tree.
	 */
	public BTreeNode constructTree() {
		ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> leafRef = this.retrieveSortedMapping();

		// Construct leaf node layer
		ArrayList<BTreeNode> leafLayer = this.createLeafLayer(leafRef);
		allNodeLevels.add(leafLayer);
		ArrayList<BTreeNode> currIndexLayer = this.createIndexLayer(leafLayer);
		allNodeLevels.add(currIndexLayer);
		while (currIndexLayer.size() > 1) {
			ArrayList<BTreeNode> prev = currIndexLayer;
//			System.out.println(currIndexLayer.size());
			ArrayList<BTreeNode> upperLevel = this.createIndexLayer(prev);
			allNodeLevels.add(upperLevel);
			currIndexLayer = upperLevel;
//			System.out.println(currIndexLayer.size());
//			System.out.println("=======================");

		}

		return currIndexLayer.get(0);
	}

	/**
	 * Creates an index layer given the list of references from the previous
	 * 
	 * @param references
	 * @return
	 */
	public ArrayList<BTreeNode> createIndexLayer(ArrayList<BTreeNode> references) {

		// Tells us the last key that has not been used.
		int counter = 0;
		int remaining = references.size();
		ArrayList<BTreeNode> indexList = new ArrayList<BTreeNode>();

		while (remaining > 0 && !((remaining > ((2 * this.order) + 1)) && remaining < (3 * this.order + 2))) {
			ArrayList<BTreeNode> childNodes = new ArrayList<BTreeNode>();
			TreeMap<Integer, ArrayList<Integer>> pointers = new TreeMap<Integer, ArrayList<Integer>>();
			int numElements = Math.min(remaining, (2 * this.order) + 1);
			int upperBound = counter + numElements;

			BTreeNode prev = null;
			for (int j = counter; j < upperBound; j++) {
				childNodes.add(references.get(j));

				if (j != counter) {
					int keyValue = references.get(j).getSmallest();
					// Each key value is unique the value should not have appeared before
					if (pointers.containsKey(keyValue)) {
//						System.out.println("Entered this part even though we are not suppose to");
						continue;
					} else {
						ArrayList<Integer> temp = new ArrayList<Integer>();
						pointers.put(keyValue, temp);

						if (j == upperBound - 1) {
							pointers.get(keyValue).add(prev.getAddress());
							pointers.get(keyValue).add(references.get(j).getAddress());
						} else {
							pointers.get(keyValue).add(prev.getAddress());
						}

					}
				}
				prev = references.get(j);
			}

			ArrayList<Map.Entry<Integer, ArrayList<Integer>>> pointerInfo = new ArrayList<Map.Entry<Integer, ArrayList<Integer>>>(
					pointers.entrySet());
//			System.out.println(this.getPageTracker());
			BTreeIndexNode index = new BTreeIndexNode(this.order, childNodes, pointerInfo, this.getPageTracker());
			indexList.add(index);
			this.pageTracker += 1;

			counter = upperBound;
			remaining = remaining - numElements;
		}

		if (remaining > 0) {
			int half = remaining / 2;
			ArrayList<BTreeNode> childNodeOne = new ArrayList<BTreeNode>();
			TreeMap<Integer, ArrayList<Integer>> pointersOne = new TreeMap<Integer, ArrayList<Integer>>();

			BTreeNode prev = null;
			for (int k = counter; k < counter + half; k++) {
				childNodeOne.add(references.get(k));
				if (k != counter) {
					int keyValue = references.get(k).getSmallest();
					// Each key value is unique the value should not have appeared before
					if (pointersOne.containsKey(keyValue)) {
//						System.out.println("Entered this part even though we are not suppose to");
						continue;
					} else {
						ArrayList<Integer> temp = new ArrayList<Integer>();
						pointersOne.put(keyValue, temp);

						if (k == counter + half - 1) {
							pointersOne.get(keyValue).add(prev.getAddress());
							pointersOne.get(keyValue).add(references.get(k).getAddress());
						} else {
							pointersOne.get(keyValue).add(prev.getAddress());
						}

					}
				}
				prev = references.get(k);
			}
			ArrayList<Map.Entry<Integer, ArrayList<Integer>>> pointerInfo = new ArrayList<Map.Entry<Integer, ArrayList<Integer>>>(
					pointersOne.entrySet());
			BTreeIndexNode index = new BTreeIndexNode(this.order, childNodeOne, pointerInfo, this.getPageTracker());
			counter = counter + half;
			this.pageTracker += 1;
			indexList.add(index);

			ArrayList<BTreeNode> childNodeTwo = new ArrayList<BTreeNode>();
			TreeMap<Integer, ArrayList<Integer>> pointersTwo = new TreeMap<Integer, ArrayList<Integer>>();

			BTreeNode prev2 = null;
			for (int l = counter; l < references.size(); l++) {
				childNodeTwo.add(references.get(l));

				if (l != counter) {
					int keyValue = references.get(l).getSmallest();

					if (pointersTwo.containsKey(keyValue)) {
//						System.out.println("Some error happen we should not be entering this loop all keys are unique");
						continue;
					}
					ArrayList<Integer> temp = new ArrayList<Integer>();
					pointersTwo.put(keyValue, temp);

					if (l == references.size() - 1) {
						pointersTwo.get(keyValue).add(prev.getAddress());
						pointersTwo.get(keyValue).add(references.get(l).getAddress());
					} else {
						pointersTwo.get(keyValue).add(prev.getAddress());
					}

				}
			}

			ArrayList<Map.Entry<Integer, ArrayList<Integer>>> pointerInfoTwo = new ArrayList<Map.Entry<Integer, ArrayList<Integer>>>(
					pointersTwo.entrySet());
			BTreeIndexNode indexTwo = new BTreeIndexNode(this.order, childNodeTwo, pointerInfoTwo,
					this.getPageTracker());
			this.pageTracker += 1;
			indexList.add(indexTwo);
		}

		return indexList;

	}

	/**
	 * A method to create the leafLayer for the index tree.
	 * 
	 * @param mapping
	 * @return
	 */
	public ArrayList<BTreeNode> createLeafLayer(ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> mapping) {
		// The counter will tell us the last key that we have not used.
		int counter = 0;
		int remaining = mapping.size();

		// Ok so we make an arraylist of leaf nodes at the bottom of the tree and there
		// will be as many of
		// them that we need to fit all of the keys in the entry.
		ArrayList<BTreeNode> leafList = new ArrayList<BTreeNode>();

		// If more than 0 keys left then there is some leaf node to make. If the
		// remaining is between 2d and 3d then stop
		// we need to split the number of nodes between them otherwise the second one
		// will be underfull.
		while (remaining > 0 && !((remaining > (2 * this.order)) && remaining < (3 * this.order))) {
			ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> references = new ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>>();
			// Either have <=2d elements or we have >=3d elements in this loop
			int numElements = Math.min(remaining, 2 * this.order);
			int upperBound = counter + numElements;

			// Smallest value in a leaf node is the first key
			int smallestValue = mapping.get(counter).getKey();

			for (int j = counter; j < upperBound; j++) {
				references.add(mapping.get(j));
			}

			// Given this list of data entries make the leaf node.
			BTreeLeafNode leaf = new BTreeLeafNode(this.clustered, this.order, references, null, smallestValue,
					this.pageTracker);
			leafList.add(leaf);

			// Update the counter to be the upper bound value now
			counter = upperBound;
			remaining = remaining - numElements;
			this.pageTracker += 1;
		}

		// This means that it was not evenly distributed and last two nodes must share
		// the reamaining amount of entries that are left.
		if (remaining > 0) {
			int half = remaining / 2;
			ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> firstNode = new ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>>();
			int firstNodeSmallestValue = mapping.get(counter).getKey();
			for (int k = counter; k < counter + half; k++) {
				firstNode.add(mapping.get(k));
			}
			// Make the leaf node and then add it to the set of leaf nodes.
			BTreeLeafNode leaf1 = new BTreeLeafNode(this.clustered, this.order, firstNode, null, firstNodeSmallestValue,
					pageTracker);
			leafList.add(leaf1);
			counter = counter + half;
			this.pageTracker += 1;

			ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> secondNode = new ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>>();
			int secondSmallest = mapping.get(counter).getKey();
			for (int p = counter; p < mapping.size(); p++) {
				secondNode.add(mapping.get(p));
			}

			BTreeLeafNode leaf2 = new BTreeLeafNode(this.clustered, this.order, secondNode, null, secondSmallest,
					pageTracker);
			this.pageTracker += 1;
			leafList.add(leaf2);

		}

		return leafList;

	}

	/**
	 * A method to get all of the keys needed when the index is not clustered.
	 * 
	 * @param reader The binary tuple reader
	 * @return
	 */
	public ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> retrieveSortedMapping() {
		int currentPage = 0;
		int currentTuple = 0;
		Tuple curr = this.reader.nextTuple();
		TreeMap<Integer, ArrayList<TupleIdentifier>> allTupleOrderings = new TreeMap<Integer, ArrayList<TupleIdentifier>>();
		while (curr != null) {
			// Get the value for the current tuple on the current index
			int value = Integer.parseInt(curr.getTuple().get(indexedColumn));

			if (allTupleOrderings.containsKey(value)) {
				TupleIdentifier temp = new TupleIdentifier(currentPage, currentTuple);
				allTupleOrderings.get(value).add(temp);
				currentTuple = currentTuple + 1;
			} else {
				TupleIdentifier temp = new TupleIdentifier(currentPage, currentTuple);
				ArrayList<TupleIdentifier> temp2 = new ArrayList<TupleIdentifier>();
				allTupleOrderings.put(value, temp2);
				allTupleOrderings.get(value).add(temp);
				currentTuple = currentTuple + 1;
			}

			if (this.reader.getTuplesLeft() == 0) {
				currentPage = currentPage + 1;
				currentTuple = 0;
			}

			curr = this.reader.nextTuple();

		}
		ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> ret = new ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>>(
				allTupleOrderings.entrySet());

		return ret;
	}

	// Need to get the serialization for the trees working.

}
