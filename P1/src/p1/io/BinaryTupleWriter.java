package p1.io;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import p1.util.Tuple;

/**
 * A tuple writer that writes binary file output.
 */
public class BinaryTupleWriter implements TupleWriter {

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

	/**
	 * Creates a ByteBuffer that writes to the output file.
	 *
	 * @param file the binary file to write to
	 * @throws IOException
	 */
	public BinaryTupleWriter(String file) {
		try {
			fout = new FileOutputStream(file);
			fc = fout.getChannel();
			numBytesLeft = 4096 - 8;
			numTuples = 0;
			idx = 8;
			bb = ByteBuffer.allocate(4096);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Writes a tuple to output.
	 *
	 * @param t the tuple to write out.
	 * @throws IOException
	 */
	@Override
	public void writeTuple(Tuple t) {
		numAttr = t.getTuple().size();
		// Reset buffer when it has written an entire page.
		if (numBytesLeft < numAttr * 4) {
			dump();
			numBytesLeft = 4096 - 8;
			numTuples = 0;
			idx = 8;
		}
		// Write the next tuple.
		for (int i = 0; i < numAttr; i++) {
			bb.putInt(idx, Integer.parseInt(t.getTuple().get(i)));
			idx += 4;
		}
		numBytesLeft -= 4 * numAttr;
		numTuples++;
	}

	/**
	 * Closes the writer.
	 *
	 * @throws IOException
	 */
	public void close() {
		try {
			dump();
			fout.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Resets the writer.
	 *
	 * @throws IOException
	 */
	public void reset() {
		try {
			fc.position(0);
			numBytesLeft = 4096 - 8;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Output the contents of the buffer.
	 *
	 * @throws IOException
	 */
	private void dump() {
		try {
			bb.putInt(0, numAttr); // Number of columns.
			bb.putInt(4, numTuples); // Number of rows in one page.
			if (numBytesLeft != 0) { // Fill in the rest of the page with zeroes.
				while (idx < 4096) {
					bb.putInt(idx, 0);
					idx += 4;
				}
			}
			fc.write(bb);
			bb = ByteBuffer.allocate(4096);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
