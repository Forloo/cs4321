package p1.logicaloperator;

/**
 * A class denoting the general class of logical operators and makes it
 * convenient for recursion and other methods later on.
 */
public abstract class LogicalOperator {

	// This is just for testing and knowing that we have the right node placement.
	public abstract String toString();

	/**
	 * Gets the string to print for the logical plan
	 * 
	 * @param level the level of the operator
	 * @return the logical plan in string form
	 */
	public abstract String toString(int level);
}
