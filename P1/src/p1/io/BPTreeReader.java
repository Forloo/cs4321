package p1.io;

import java.io.FileInputStream;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import p1.index.BTreeIndexNode;
import p1.index.BTreeLeafNode;
import p1.index.BTreeNode;
import p1.index.TupleIdentifier;
import p1.util.Tuple;

/**
 * A tree reader that reads binary input.
 */
public class BPTreeReader {

	// Obtains input bytes from a file in a file system.
	FileInputStream fin;
	// The file channel.
	private FileChannel fc;
	// The buffer that reads one file page at a time. Each page is 4096 bytes.
	private ByteBuffer bb;

	private ArrayList<Integer> header;
	private int numKeys;
	private int idx = 0;
	private int dataEnt;
	private int numEl;
	private int curDatEnt = 0;
	private HashMap<Integer, ArrayList<ArrayList<Integer>>> pair = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();
	private ArrayList<ArrayList<Integer>> locations = new ArrayList<ArrayList<Integer>>();
	private Tuple tuples = new Tuple("");
	private int key;
	private String file;
	
	private static final int pageSize=4096;

	/**
	 * Creates a ByteBuffer that reads from the input file
	 * 
	 * @param file of the binary file to read from
	 * @throws IOException
	 */
	public BPTreeReader(String file) {
		try {
			this.file = file;
			fin = new FileInputStream(file);
			fc = fin.getChannel();
			bb = ByteBuffer.allocate(4096);
			fc.read(bb);
			ArrayList<Integer> temp = new ArrayList<Integer>();
			for (int i = 0; i < 3; i++) {
				temp.add(bb.getInt(0));
				temp.add(bb.getInt(4));
				temp.add(bb.getInt(8));
				header = temp;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void goToFirstLeaf() {
		reset(1);
		checkNodeType();
	}
	
	/**
	 * Retrieves a BTreeNode given the page to deserialize 
	 * @return BTreeNode for the given page.
	 */
	public BTreeNode deserializeNode() {
		int type = bb.getInt(0);
		if (type==0) {
			BTreeNode leaf = deserializeLeafNode();
			return leaf;
		}
		else {
			BTreeNode index= deserializeIndexNode();
			return index;
		}
	}
	
	/**
	 * Constructs a BTreeIndexNode from the binary file given thatt the type of 
	 * the page being represented is a leaf node.
	 * @return
	 */
	public BTreeIndexNode deserializeIndexNode() {
		// Do not take in the first byte instead read from the second byte for the page
		
		// The first byte here tells us the number of keys that we have in the table
		int pos=4;
		int numKeys=bb.getInt(pos);
		pos+=4;
		ArrayList<Integer> allKeys= new ArrayList<Integer>();
		TreeMap<Integer,ArrayList<Integer>> mappingInformation= new TreeMap<Integer,ArrayList<Integer>>();
		for(int i=0;i<numKeys;i++) {
			int curr=bb.getInt(pos);
			allKeys.add(curr);
			pos+=4;
		}
		
		// Given the number of keys then we know that there is always one more address then the number 
		// of keys that we have
		
		for(int j=0;j<numKeys+1;j++) {
			// If the entry is not the last one then put it in the  map a
			if (j!=numKeys) {
				ArrayList<Integer> addressValues = new ArrayList<Integer>();
				addressValues.add(bb.getInt(pos));
				pos+=4;
				mappingInformation.put(allKeys.get(j), addressValues);
			}
			else {
				mappingInformation.get(allKeys.get(j-1)).add(bb.getInt(pos));
				pos+=4;
			}
		}
		
		// After getting all of the values we make the node with all the referencing information that we need.
		ArrayList<Map.Entry<Integer, ArrayList<Integer>>> ret = new ArrayList<Map.Entry<Integer, ArrayList<Integer>>>(
				mappingInformation.entrySet());
		
		// This value does not matter since wee only need the reference for this information
		int order=0;
		int address=0;
		BTreeIndexNode curr = new BTreeIndexNode(order,null,ret,address);
		
		return curr;
		
	}
	
	/**
	 * Constructs a BTreeleafNode from the binary file given that the type of the 
	 * node being represented is a leaf node
	 * @return BTreeLeafNode for the given page.
	 * Pre-Condition: The precondition for this is that the page must be on a 
	 * leaf page.
	 */
	public BTreeLeafNode deserializeLeafNode() {
		// Other method will check the node type of the file. So we can start by 
		// Checking the second byte of the page and getting the information starting
		// from that point
		
		// The number of keys for the references in the leaf node
		int pos=4;
		int numKeys= bb.getInt(pos);
		pos+=4;
		
		TreeMap<Integer, ArrayList<TupleIdentifier>> allTupleOrderings= new TreeMap<Integer,ArrayList<TupleIdentifier>>();
		
		for(int i=0;i<numKeys;i++) {
			// Get the current key for the current entry
			int key=bb.getInt(pos);
			pos+=4;
			// Get the number of data entries for this current entry
			int numData= bb.getInt(pos);
			pos+=4;
			ArrayList<TupleIdentifier> currEntry= new ArrayList<TupleIdentifier>();
			for(int j=0;j<numData;j++) {
				// For each of the data entries read two values to get the pageId and the tupleid
				int page=bb.getInt(pos);
				pos+=4;
				int tupleNumber=bb.getInt(pos);
				pos+=4;
				TupleIdentifier curr= new TupleIdentifier(page,tupleNumber);
				currEntry.add(curr);
			}
			
			allTupleOrderings.put(key, currEntry);
		}
		ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> ret = new ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>>(
				allTupleOrderings.entrySet());
		
		// Information here does not matter since the clustering is done beforehand.
		boolean clustered= true;
		// The order here does not matter since we have already made the tree
		int ordering=0;
		// The smallest value in the given subtree does not matter since that is only for construciton
		int smallest=0;
		// Getting the address does not matter here since we will have that when we are traversing the tree
		int address=0;
		
		BTreeLeafNode currentLeafNode= new BTreeLeafNode(clustered,ordering,ret,null,smallest,address);
		
		return currentLeafNode;
		
	}
	
	/**
	 * Retrieves the information from the header page.
	 * @return ArrayList<Integer> three ints telling us information about the tree.
	 */
	public ArrayList<Integer> getHeaderInfo(){
		// Only reading the portion of the buffer here that you need
		
		ArrayList<Integer> ret = new ArrayList<Integer>();
		int pos=0;
		for(int i=0;i<3;i++) {
			int curr_value=bb.getInt(pos);
			pos+=4;
			ret.add(curr_value);
		}
		
		return ret;
	}
	
	
	/**
	 * returns the node type (either leaf or inner). Call this first for each node.
	 * 
	 * @return true if the node is the leaf node, false otherwise
	 */
	public Boolean checkNodeType() {
		
//		return bb.getInt(0) == 0;
		
//		 read new page.
		try {
			bb = ByteBuffer.allocate(4096); // skip the header page afterwards
			int end = fc.read(bb);
			idx = 0;
			curDatEnt = 0;
			if (end == -1) {
				return null;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		idx += 4;
		return bb.getInt(0) == 0;
	}


	/**
	 * gets the next data entry for the leaf node (unclustered tpye). Call this
	 * after checkNodeType()
	 * 
	 * @return data entry of leaf node, null after returning all the entries.
	 */
	public HashMap<Integer, ArrayList<ArrayList<Integer>>> getNextDataEntryUnclus() {
		if (idx == 4) {
			dataEnt = bb.getInt(idx);
			idx += 4;
		}
		if (curDatEnt < dataEnt) { // leaf node starts with 0 and num entries, then entries
			key = bb.getInt(idx); // start by getting key
			locations = new ArrayList<ArrayList<Integer>>();
			pair = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();
			idx += 4;
			numEl = bb.getInt(idx); // then num elements
			idx += 4;
			for (int i = 0; i < numEl; i++) { // number of pairs
				ArrayList<Integer> onePair = new ArrayList<Integer>();
				for (int j = 0; j < 2; j++) { // per element (pair)
					onePair.add(bb.getInt(idx));
					idx += 4;
				}
				locations.add(onePair);
			}
			curDatEnt += 1;
			pair.put(key, locations);
			return pair;
		} else { // done returning the key value pairs
			return null;
		}
	}
	
	/**
	 * deserializes one leaf. Call this method while on the leaf to deserialize.
	 * 
	 * @return deserialized leaf 
	 */
	public ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> deserializeLeaf() {
		idx = 0;
		ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>> leaf = new ArrayList<HashMap<Integer, ArrayList<ArrayList<Integer>>>>();
		HashMap<Integer, ArrayList<ArrayList<Integer>>> element = getNextDataEntryUnclus();
		int i = 0;
		while( element != null) {
			i ++;
			leaf.add(element);
			element = getNextDataEntryUnclus();
		}
		idx = 4;
		return leaf;
	}
	
	/**
	 * gets the next key for the node. Call this after checkNodeType(). After
	 * calling this, call getNextAddrIN.
	 * 
	 * @return key of index node, -1 after returning all the keys.
	 */
	public int getNextKey() {
		if (idx == 4) {
			numKeys = bb.getInt(idx);
			idx += 4;
		}
		if (idx / 4 - 2 < numKeys) { // index node starts with 1 and num keys, then keys
			int ret = bb.getInt(idx);
			idx += 4;
			return ret;
		} else { // done returning the keys
			idx -= 4;
			return -1;
		}
	}

	/**
	 * gets the index node's next address for child node.
	 * 
	 * @return address of child node.
	 */
	public int getNextAddrIN() {
		if (idx >= 4096) {
//			idx = 0;
			return -1;
		} else {
			int ret = bb.getInt(idx);
			if (ret == 0) {
				return -1;
			}
			idx += 4;
			return ret;
		}
	}

	/**
	 * Closes the reader.
	 *
	 * @throws IOException
	 */
	public void close() {
		try {
			fin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets the address of the root
	 * 
	 * @return address of the root
	 */
	public int getAddressOfRoot() {
		return header.get(0);
	}

	/**
	 * Gets the number of keys in one page 
	 * 
	 * @return number of keys
	 */
	public int getNumKeys() {
		return numKeys;
	}

	/**
	 * Gets the number of leaves in the tree
	 * 
	 * @return number of leaves
	 */
	public int getNumLeaves() {
		return header.get(1);
	}

	/**
	 * Gets the order of the tree
	 * 
	 * @return order of tree
	 */
	public int getOrderOfTree() {
		return header.get(2);
	}

	/**
	 * Resets the reader to the ith page.
	 */
	public void reset(int idx) {
		try {
			fin = new FileInputStream(file);
			fc = fin.getChannel();
			for (int i = 0; i < idx; i++) {
				checkNodeType();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static int getPageSize() {
		return pageSize;
	}
	
	/**
	 * Read from the filechannel into the buffer so the information can be parse after.
	 * @param fc 
	 * @param bb
	 * @param pageNum
	 */
	public void read(int pageNum) {
		
		// Reset the values in the buffer first so that we can overwrite the values in them
		this.resetBuffer();
		try {
			int fileLocation=pageNum*BPTreeReader.getPageSize();
			// Move to the right spot in the fc location to start reading from
			fc.position(fileLocation);
			fc.read(bb);
			// Need to flip the buffer so that we change it from writing to the read mode
			bb.flip();
		}
		catch(IOException e){
			System.out.println("Error while reading from the input file");
		}
		
	}
	
	/**
	 * Reset the buffer to contain no values at all.
	 * @param buffer The input buffer that we are currently using.
	 */
	public void resetBuffer() {
		// Set the position of the buffer back to the beginning of the buffer
		bb.clear();
		// Make a new buffer with the size of the page
		byte[] fill = new byte[BPTreeReader.getPageSize()];
		bb.put(fill);
		// Clear the buffer again meaning that we start from the beginning of the buffer
		bb.clear();
		
	}
	

}
