package p1.operator;

import java.util.ArrayList;

import p1.util.Tuple;

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
	 * Resets the Operator to the ith tuple.
	 */
	public abstract void reset(int idx);

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the table.
	 */
	public abstract ArrayList<String> getSchema();

	/**
	 * Gets the table name.
	 *
	 * @return the table name.
	 */
	public abstract String getTable();

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

	/**
	 * Gets the string to print for the physical plan
	 * 
	 * @param level the level of the operator
	 * @return the physical plan in string form
	 */
	public abstract String toString(int level);

}
