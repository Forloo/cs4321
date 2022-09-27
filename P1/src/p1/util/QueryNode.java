package p1.util;

import java.util.ArrayList;

import p1.operator.Operator;

public class QueryNode {

	// Operator for the current node
	private Operator op;

	// The nodes left child
	private QueryNode left;

	// The nodes right child
	private QueryNode right;

	// The arraylist of tuples for this given node
	private ArrayList<Tuple> rowValues;

	public QueryNode(Operator op, QueryNode left, QueryNode right) {
		this.op = op;
		this.left = left;
		this.right = right;
	}

	/**
	 * Retrieves the operator for the current node
	 *
	 * @return The operator for the current node
	 */
	public Operator getOperator() {
		return op;
	}

	/**
	 * Retrieves the left child for the current node
	 *
	 * @return A querynode or null if there is no left child
	 */
	public QueryNode leftChild() {
		return left;
	}

	/**
	 * Retrieves the right child for the current node
	 *
	 * @return A querynode or null if there is no right child.
	 */
	public QueryNode rightChild() {
		return right;
	}

	/**
	 * Sets the left child for the current node.
	 *
	 * @param left: The new left child
	 */
	public void setLefttChild(QueryNode left) {
		this.left = left;
	}

	/**
	 * Sets the right child for the current node
	 *
	 * @param right: The neew right child.
	 */
	public void setRightChild(QueryNode right) {
		this.right = right;
	}

	/**
	 * Returns the string representation of this node
	 *
	 * @param root The current query node
	 * @return A string representing the current nodes operator.
	 */
	public String toString(QueryNode root) {
		return op.toString();
	}

	/**
	 * Get the list of tuples corresponding to this node
	 *
	 * @return
	 */
	public ArrayList<Tuple> getTuples() {
		return rowValues;
	}

	/**
	 * Set the list of tuples for this node
	 *
	 * @param valueList: The new list of tuples for this row.
	 */
	public void setTuples(ArrayList<Tuple> valueList) {
		rowValues = valueList;
	}

}
