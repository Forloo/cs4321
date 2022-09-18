package p1.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import p1.util.Tuple;

/**
 * A tuple writer that writes text file output.
 */
public class HumanTupleWriter implements TupleWriter {

	// File writer.
	private PrintWriter out;

	/**
	 * Creates a PrintWriter that writes to the output file.
	 *
	 * @param file the human-readable file to write to
	 * @throws IOException
	 */
	public HumanTupleWriter(String fileLoc) {
		try {
			out = new PrintWriter(fileLoc);
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
		out.println(t.toString());
	}

	/**
	 * Closes the writer.
	 *
	 * @throws IOException
	 */
	public void close() {
		out.close();
	}

}
