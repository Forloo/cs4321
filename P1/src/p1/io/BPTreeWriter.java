package p1.io;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
	
	
	/**
	 * Creates a ByteBuffer that writes to the output file.
	 *
	 * @param file the binary file to write to
	 * @throws IOException
	 */
	public BPTreeWriter(String file) {
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
}
