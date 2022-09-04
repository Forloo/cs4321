package p1.operator;

import p1.Tuple;

/**
 * An abstract class used to implement all relational operators with an iterator
 * API.
 */
public abstract class Operator {

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public abstract Tuple getNextTuple();

	/**
	 * Resets the Operator to its original state
	 */
	public abstract void reset();

	/**
	 * Retrieves all tuples and outputs them to stdout.
	 */
	public abstract void dump();

	/**
	 * Retrieves all tuples and outputs them to stdout.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	public abstract void dump(String outputFile);

}
