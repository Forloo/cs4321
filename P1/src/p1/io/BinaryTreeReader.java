package p1.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;

import p1.util.Tuple;

/**
 * A tree reader that reads binary input.
 */
public class BinaryTreeReader {
	//4096 byte page, each node fits within
	//first is header page
	//header -> leaf node -> inner node above -> above -> ... -> root node
	//address of node: number of page it is serialized on
	
	//index node1 flag
	//	number of keys in node
	//	actual keys in node in order
	//  the addresses of all children of the node in order
	
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
	private int key;
	/**
	 * Creates a ByteBuffer that reads from the input file
	 * 
	 * @param file of the binary file to read from
	 * @throws IOException
	 */
	public BinaryTreeReader(String file) {
		try {
			fin = new FileInputStream(file);
			fc = fin.getChannel();
			bb = ByteBuffer.allocate(4096);
			fc.read(bb);
			for(int i = 0; i < 3; i++) {
				header.add(bb.getInt(0));
				header.add(bb.getInt(4));
				header.add(bb.getInt(8));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * returns the node type (either leaf or inner). Call this first
	 * for each node. 
	 * @return true if the node is the leaf node, false otherwise
	 */
	public Boolean checkNodeType() {
		//read new page.
		try {
			bb = ByteBuffer.allocate(4096); //skip the header page afterwards
			fc.read(bb);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		idx += 4;
		return bb.get(0) == 0;
	}
	
	/**
	 * gets the next data entry for the leaf node (clustered type). Call this after checkNodeType()
	 * @return data entry of leaf node, null after returning all the entries.
	 */
	public HashMap<Integer, ArrayList<ArrayList<Integer>>> getNextDataEntryClus() {
		return null;
	}
	
	/**
	 * gets the next data entry for the leaf node (unclustered tpye). Call this after checkNodeType()
	 * @return data entry of leaf node, null after returning all the entries.
	 */
	public HashMap<Integer, ArrayList<ArrayList<Integer>>> getNextDataEntryUnclus() {
		if (idx == 4) {
			dataEnt = bb.get(idx);
			idx += 4;
		}
		if (curDatEnt < dataEnt) { //leaf node starts with 0 and num entries, then entries
			key = bb.get(idx); //start by getting key
			idx += 4;
			numEl = bb.get(idx); //then num elements 
			idx += 4;
			for (int i = 0; i < numEl; i++) { //number of pairs
				ArrayList<Integer> onePair = new ArrayList<Integer>();
				for (int j =0; j < 2; j ++) { //per element (pair)
					onePair.add((int) bb.get(idx));
					idx += 4;
				}
				locations.add(onePair);
			}
			curDatEnt += 1;
			pair.put(key, locations);
			return pair;
		} else { //done returning the keys
			return null;
		}
	}
	
	/**
	 * gets the next key for the node. Call this after checkNodeType()
	 * @return key of index node, -1 after returning all the keys.
	 */
	public int getNextKey() {
		if (idx == 4) {
			numKeys = bb.get(idx);
			idx += 4;
		}
		if (idx / 4 - 2 < numKeys) { //index node starts with 1 and num keys, then keys
			int ret = bb.get(idx);
			idx += 4;
			return ret;
		} else { //done returning the keys
			return -1;
		}
	}
	
	/**
	 * gets the index node's next address for child node.
	 * @return address of child node.
	 */
	public int getNextAddrIN() {
		if(idx>4096) {
			idx = 0;
			return -1;
		} else {
			int ret = bb.get(idx);
			idx += 4;
			return ret;
		}
	}
	
	
	/**
	 * Gets the address of the root
	 * @return address of the root
	 */
	public int getAddressOfRoot() {
		return header.get(0);
	}
	
	/**
	 * Gets the number of leaves in the tree
	 * @return number of leaves
	 */
	public int getNumLeaves() {
		return header.get(1);
	}
	
	/**
	 * Gets the order of the tree
	 * @return order of tree
	 */
	public int getOrderOfTree() {
		return header.get(2);
	}
	
}
