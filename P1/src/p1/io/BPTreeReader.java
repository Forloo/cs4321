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
public class BPTreeReader{
	
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
			ArrayList<Integer> temp = new ArrayList<Integer>();
			for(int i = 0; i < 3; i++) {
				temp.add(bb.getInt(0));
				temp.add(bb.getInt(4));
				temp.add(bb.getInt(8));
				header = temp;
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
//		System.out.println("is leaf node: " +  bb.getInt(0));
		return bb.getInt(0) == 0;
	}
	
	/**
	 * gets the next data entry for the leaf node (clustered type). Call this after checkNodeType()
	 * @return data entry of leaf node, null after returning all the entries.
	 */
	public HashMap<Integer, ArrayList<ArrayList<Integer>>> getNextDataEntryClus() {
//		if (idx == 4) {
//			dataEnt = bb.getInt(idx);
//			idx += 4;
//		}
//		
//		if (curDatEnt < dataEnt) {
//			key = bb.getInt(idx); //start by getting key
//			locations = new ArrayList<ArrayList<Integer>>(); //stores tuples
//			pair = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();
//			System.out.println("key: " + key);
//			idx += 4;
//			numEl = bb.getInt(idx); //then num elements
//			System.out.println("numEl: " + numEl);
//			idx += 4;
//		}
		
		return null;
	}
	
	/**
	 * gets the next data entry for the leaf node (unclustered tpye). Call this after checkNodeType()
	 * @return data entry of leaf node, null after returning all the entries.
	 */
	public HashMap<Integer, ArrayList<ArrayList<Integer>>> getNextDataEntryUnclus() {
		if (idx == 4) {
			dataEnt = bb.getInt(idx);
			idx += 4;
		}
//		System.out.println(dataEnt);
		if (curDatEnt < dataEnt) { //leaf node starts with 0 and num entries, then entries
			key = bb.getInt(idx); //start by getting key
			locations = new ArrayList<ArrayList<Integer>>();
			pair = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();
			System.out.println("key: " + key);
			idx += 4;
			numEl = bb.getInt(idx); //then num elements 
			System.out.println("numEl: " + numEl);
			idx += 4;
			for (int i = 0; i < numEl; i++) { //number of pairs
				ArrayList<Integer> onePair = new ArrayList<Integer>();
				for (int j =0; j < 2; j ++) { //per element (pair)
					onePair.add(bb.getInt(idx));
					idx += 4;
				}
				locations.add(onePair);
			}
			curDatEnt += 1;
			pair.put(key, locations);
			System.out.println(pair.toString());
			return pair;
		} else { //done returning the key value pairs
			return null;
		}
	}
	
	/**
	 * gets the next key for the node. Call this after checkNodeType(). After calling this,
	 * call getNextAddrIN.
	 * @return key of index node, -1 after returning all the keys.
	 */
	public int getNextKey() {
//		System.out.println("current idx for next key is: " + idx);
		if (idx == 4) {
			numKeys = bb.getInt(idx);
//			System.out.println("number of keys: "+numKeys);
			idx += 4;
		}
		if (idx / 4 - 2 < numKeys) { //index node starts with 1 and num keys, then keys
			int ret = bb.getInt(idx);
			idx += 4;
//			System.out.println(ret);
			return ret;
		} else { //done returning the keys
			idx -= 4;
			return -1;
		}
	}
	
	/**
	 * gets the index node's next address for child node.
	 * @return address of child node.
	 */
	public int getNextAddrIN() {
//		System.out.println("start idx: " + idx);
//		System.out.println("cur idx is: "+ idx);
		if(idx>=4096) {
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
