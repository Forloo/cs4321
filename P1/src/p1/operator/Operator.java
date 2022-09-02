package p1.operator;

import p1.Tuple;

public abstract class Operator {
	
	/*
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 */
	public abstract Tuple getNextTuple();
	
	/*
	 * Resets the Operator to its original state 
	 */
	public abstract void reset();
	
	/*
	 * Retrieves all tuples and outputs them to stdout.
	 */
	public abstract void dump();
	
}
