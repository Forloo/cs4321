package p1.io;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Map;

import p1.index.BTreeIndexNode;
import p1.index.BTreeLeafNode;
import p1.index.BTreeNode;
import p1.index.TupleIdentifier;

public class BPTreeWriter {

	// File output.
	FileOutputStream fout;
	// The file channel to write to.
	private FileChannel fc;
	// The buffer that writes one file page at a time. Each page is 4096 bytes.
	private ByteBuffer bb;
	// Number of attributes of one row.
	private int numAttr;
	// Number of bytes left to write on one page.
	private int numBytesLeft;
	// Number of rows on one page.
	private int numTuples;
	// Current buffer index.
	private int idx;
	
	//private arraylist<Arraylist<BTreeNode>>
	// The 0th index is the leaf node and all other indexes following that will be
	// index layer nodes
	
	/**
	 * Creates a ByteBuffer that writes to the output file.
	 *
	 * @param file the binary file to write to
	 * @throws IOException
	 */
	public BPTreeWriter(ArrayList<ArrayList<BTreeNode>> gpt, File indexFile, BTreeNode bpTree, int order) {
		try {
			//initialize fileoutputStream
			fout = new FileOutputStream(indexFile);
			fc = fout.getChannel();
			writeHeader(bpTree, gpt, order);
			for(ArrayList<BTreeNode> typeOfNode : gpt) {
				for(BTreeNode eachNode : typeOfNode) {
					if(eachNode instanceof BTreeLeafNode) {
						idx = 0;
//						System.out.println("writing to leaf");
						writeLeafNode(eachNode);
					} else { //index node, and root
						idx = 0;
						writeIndexNode(eachNode);
					}
				}
			}
			fout.close(); //close after serializing the tree
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeHeader(BTreeNode bpTree, ArrayList<ArrayList<BTreeNode>> gpt, int order) {
		bb = ByteBuffer.allocate(4096); 
		//write header page
		idx = 0;
		bb.putInt(idx, bpTree.getAddress() + 1); //write address of root
		idx += 4;
		bb.putInt(idx, gpt.get(0).size()); //write number of leaves
		idx += 4;
		bb.putInt(idx, order); //writer order
		idx += 4;
		while(idx < 4096) { //fill with zeros
			bb.putInt(idx, 0);
			idx += 4;
		}
		try {
			fc.write(bb);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//allocate bytebuffer
	}
	/**
	 * writes the index node in the byte buffer
	 * 
	 * @param idxn is the index node
	 */
	public void writeIndexNode(BTreeNode idxn) {
		bb = ByteBuffer.allocate(4096); //allocate space for more
		BTreeIndexNode idexN = (BTreeIndexNode) idxn;
		bb.putInt(idx, 1); //1 for index node
		idx += 4;
		bb.putInt(idx,idexN.getReferenceSize()); //num keys
		idx += 4;
		
		//write keys
		for(Map.Entry<Integer,ArrayList<Integer>> mp : idexN.getReferences()) {
			bb.putInt(idx, mp.getKey());
			idx += 4;
		}
		
		//write child address
		for(Map.Entry<Integer,ArrayList<Integer>> mp : idexN.getReferences()) {
			for(int childAddr : mp.getValue()) {
				bb.putInt(idx, childAddr + 1);
				idx += 4;
			}
		}
		
			
		try {
			while(idx < 4096) {
				bb.putInt(idx, 0);
				idx += 4;
			}
			fc.write(bb); //write one leaf node
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * writes the leaf node in the byte buffer
	 * @param lfn is the leaf node
	 */
	public void writeLeafNode(BTreeNode lfn) {
		bb = ByteBuffer.allocate(4096); //allocate space for more
		BTreeLeafNode leafNd = (BTreeLeafNode) lfn;
		//write 0 first for leaf
		bb.putInt(idx, 0);
		idx += 4;
		// write number of entries
		bb.putInt(idx,leafNd.getReferenceSize());
		idx += 4;
		//write key, number of pairs, and pairs in order
		for(Map.Entry<Integer, ArrayList<TupleIdentifier>> mp : leafNd.getReference() ) {
			//write key
//			System.out.println(mp.getValue());
			bb.putInt(idx, mp.getKey());
			idx += 4;
			//write number of pairs
			bb.putInt(idx, mp.getValue().size());
			idx += 4;
			//write the pairs
			for(TupleIdentifier tp : mp.getValue()) {
				bb.putInt(idx, tp.getPageId());
				idx += 4;
				bb.putInt(idx, tp.getTupleId());
				idx += 4;
			}
		}
		try { 
			while(idx < 4096) {
				bb.putInt(idx, 0);
				idx += 4;
			}
			fc.write(bb); //write one leaf node
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
