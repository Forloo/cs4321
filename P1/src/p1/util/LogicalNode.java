package p1.util;

import p1.logicaloperator.LogicalOperator;

public class LogicalNode {
	
	// Logical Operator for the current node
	private LogicalOperator op;
	// The nodes left child
	private LogicalNode left;
	// The nodes right child
	private LogicalNode right;
	
	/**
	 * Constructor for a LogicalNode
	 * @param op The logical operator for the current node
	 * @param left The left child
	 * @param right The right child
	 */
	public LogicalNode(LogicalOperator op, LogicalNode left, LogicalNode right) {
		this.op=op;
		this.left=left;
		this.right=right;
	}
	
	/**
	 * Retrieves the logical operator
	 * @return A logicalOperator
	 */
	public LogicalOperator getLogicalOperator() {
		return op;
	}
	
	/**
	 * Retrieves the logical Nodes left child
	 * @return A logical node
	 */
	public LogicalNode leftChild() {
		return left;
	}
	
	/**
	 * Retrieves the logical nodes right child
	 * @return A logicalNode
	 */
	public LogicalNode rightChild() {
		return right;
	}
	
	/**
	 * Updates the left child for the current node
	 * @param left The new left child
	 */
	public void setLeftChild(LogicalNode left) {
		this.left=left;
	}
	
	/**
	 * Updates the right child for the current node
	 * @param right The logical nodes right child.
	 */
	public void setRightChild(LogicalNode right) {
		this.right=right;
	}
	
	/**
	 * Return a string of the logical node
	 */
	public String toString() {
		return this.getLogicalOperator().toString();
	}
}
