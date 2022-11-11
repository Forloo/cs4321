package p1.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import p1.index.TupleIdentifier;
import p1.util.Tuple;

/**
 * A tuple reader that reads binary input.
 */
public class BinaryTupleReader implements TupleReader {

	// Obtains input bytes from a file in a file system.
	FileInputStream fin;
	// The file channel.
	private FileChannel fc;
	// The buffer that reads one file page at a time. Each page is 4096 bytes.
	private ByteBuffer bb;
	// Number of attributes of one row.
	private int numAttr;
	// Number of tuples left to read on one page.
	private int numTuplesLeft;
	// Current buffer index.
	private int idx;
	// The current page 
	private int currPage;
	// /The current tuple
	private int currTuple;

	/**
	 * Creates a ByteBuffer that reads from the input file.
	 *
	 * @param file the binary file to read from
	 * @throws IOException
	 */
	public BinaryTupleReader(String file) {
		try {
			fin = new FileInputStream(file);
			fc = fin.getChannel();
			bb = ByteBuffer.allocate(4096);
			fc.read(bb);
			numAttr = bb.getInt(0);
			numTuplesLeft = bb.getInt(4);
			idx = 8;
			currPage=0;
			currTuple=-1;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read the next tuple.
	 *
	 * @return the next tuple.
	 * @throws IOException
	 */
	@Override
	public Tuple nextTuple() {
		try {
			// Reset buffer when it has read an entire page.
			if (numTuplesLeft == 0) {
				bb = ByteBuffer.allocate(4096);
				int end;
				end = fc.read(bb);
				if (end == -1) {
					return null;
				}
				numAttr = bb.getInt(0);
				numTuplesLeft = bb.getInt(4);
				idx = 8;
				currPage+=1;
				currTuple=-1;
			}
			// Read the next tuple.
			ArrayList<String> attr = new ArrayList<String>();
			for (int i = 0; i < numAttr; i++) {
				attr.add(String.valueOf(bb.getInt(idx)));
				idx += 4;
			}
			numTuplesLeft--;
			currTuple+=1;
			return new Tuple(String.join(",", attr));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Read the next tuple given pageID and tupleID.
	 *
	 * @return the next tuple.
	 * @throws IOException
	 */
	@Override
	public Tuple nextTupleIndex(TupleIdentifier currRid, int pageId, int tupleId) throws IOException {
        if (currRid == null) {
        	return null;
        }       
        
        if (currPage > pageId) {
        	reset();
        } 
        else if (currPage == pageId) {
        	if (currTuple > tupleId) {
        		reset();
        	}
        }
        
        
        int pagepos = (tupleId * numAttr + 2) * 4; // set position on page 
        bb.position(pagepos);        
        
        return nextTuple();
    } 
	
	public Tuple nextTupleIndex(int pageId,int tupleId) {
		if (currPage>pageId) {
			reset();
		}
		else if (currPage==pageId) {
			if(currTuple>tupleId) {
				reset();
			}
		}
		
		while (currPage<pageId) {
			Tuple currentTuple=nextTuple();
			// Handle the edge case when the page switches over but we start
			// on the zeroth tuple.
			if (currPage==pageId && currTuple==tupleId) {
				return currentTuple;
			}
		}
		
		Tuple tupleRet=null;
		while(currTuple<tupleId) {
			tupleRet=nextTuple();
			if (currTuple==tupleId) {
				return tupleRet;
			}
		}
		return null;
		
	}

	/**
	 * Closes the reader.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() {
		try {
			fin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Resets the reader.
	 *
	 * @throws IOException
	 */
	@Override
	public void reset() {
		try {
			fc.position(0);
			numTuplesLeft = 0;
			currPage=-1;
			currTuple=0;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Resets the reader to the ith tuple.
	 */
	public void reset(int idx) {
		try {
			fc.position(0);
			numTuplesLeft = 0;
			for (int i = 0; i < idx; i++) {
				nextTuple();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Retrieves the number of tuple left on the current page
	 * @return An integer telling us the number of tuple left on this page.
	 */
	public int getTuplesLeft() {
		return numTuplesLeft;
	}


}
