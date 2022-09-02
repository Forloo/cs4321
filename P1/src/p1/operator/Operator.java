package p1.operator;

public abstract class Operator {
	
	/*
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 */
	public abstract p1.Tuple getNextTuple();
	
	/*
	 * Resets the Operator to its original state 
	 */
	public abstract void reset();
	
	/*
	 * Retrieves all tuples and outputs them to stdout.
	 */
	public abstract void dump();
}
