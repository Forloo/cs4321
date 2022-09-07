package p1;
import p1.operator.*;

public class TreeNode {
	
	// Operator for the current node
	private Operator op;
	// The nodes left child
	private TreeNode left;
	// The nodes right child
	private TreeNode right;
	
	/**
	 * Creates a TreeNode Object representing an operator
	 * @param op The operator for the current node
	 * @param left The left child for the TreeNode
	 * @param right The right child for the TreeNode
	 */
	public TreeNode(Operator op,TreeNode left, TreeNode right) {
		this.op=op;
		this.left=left;
		this.right=right;
	}
	
	/**
	 * Retrieves the operator
	 * @return The operator of the TreeNode
	 */
	public Operator getOperator() {
		return op;
	}
	
	/**
	 * Retrieves the nodes left child
	 * @return A TreeNode. null if none exists
	 */
	public TreeNode leftChild() {
		return left;
	}
	
	/**
	 * Retrieves the nodes right child
	 * @return A TreeNode. null if the none exists.
	 */
	public TreeNode rightChild() {
		return right;
	}
	
	/**
	 * Sets the TreeNodes left child
	 * @param root: Nodes left child that is updated.
	 * @param node: The left child node.
	 */
	public void setLeftChild(TreeNode root,TreeNode node) {
		root.left=node;
	}
	
	/**
	 * Sets the TreeNodes right child
	 * @param root:Nodes right child that is updated
	 * @param node: The right child node
	 */
	public void setRightChild(TreeNode root,TreeNode node) {
		root.right=node;
	}
	
	/**
	 * Returns the string representation of TreeNode
	 * @param root: Node whose string representation is returned
	 * @return: The string representation of TreeNode
	 */
	public String toString(TreeNode root) {
		
		return op.toString();
	}
	
	/**
	 * Returns the string representation of a TreeNode.
	 */
	public String toString() {
		return op.toString();
	}
	
}
