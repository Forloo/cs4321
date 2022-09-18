package p1.io;

import java.io.IOException;

import p1.util.Tuple;

/**
 * A tuple writer.
 */
public interface TupleWriter {

	/**
	 * Writes a tuple to output.
	 *
	 * @param t the tuple to write out.
	 */
	public void writeTuple(Tuple t) throws IOException;
}
