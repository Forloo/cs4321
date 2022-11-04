package p1.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

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
	private int currPage = 0;

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
				currPage++;
				bb = ByteBuffer.allocate(4096);
				int end;
				end = fc.read(bb);
				if (end == -1) {
					return null;
				}
				numAttr = bb.getInt(0);
				numTuplesLeft = bb.getInt(4);
				idx = 8;
			}
			// Read the next tuple.
			ArrayList<String> attr = new ArrayList<String>();
			for (int i = 0; i < numAttr; i++) {
				attr.add(String.valueOf(bb.getInt(idx)));
				idx += 4;
			}
			numTuplesLeft--;
			return new Tuple(String.join(",", attr));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
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
			currPage = 0;
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
			currPage = 0;
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
