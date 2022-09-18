package p1.io;

import p1.util.Tuple;

/**
 * An interface that reads tuples with bookkeeping methods.
 */
public interface TupleReader {

	/**
	 * Read the next tuple.
	 *
	 * @return the next tuple.
	 */
	public Tuple nextTuple();

	/**
	 * Closes the reader.
	 */
	public void close();

	/**
	 * Resets the reader.
	 */
	public void reset();
}
