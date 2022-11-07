package p1.io;

import java.io.IOException;
import java.util.ArrayList;

import p1.index.TupleIdentifier;
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

	/**
	 * Resets the reader to the ith tuple.
	 */
	public abstract void reset(int idx);

	/**
	 * Read the next tuple given pageID and tupleID.
	 *
	 * @return the next tuple.
	 * @throws IOException
	 */
	Tuple nextTupleIndex(TupleIdentifier currRid, int pageId, int tupleId) throws IOException;
}
