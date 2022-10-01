package p1.util;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class QueryTree {

	// The root node of the tree
	private QueryNode root;
	
	/**
	 * The constructor for the query tree
	 */
	public QueryTree() {
		root=null;
	}
	
	// TODO this must be done and then tested to see if it is correct. This is only testable after 
	// we do some refactoring meaning changing the operators that we had originally.
	public QueryNode buildTree(PlainSelect plainSelect, DatabaseCatalog db) {
		return null;
	}
	
	/**
	 * Retrieves the root node for the tree
	 * @return A Query node 
	 */
	public QueryNode getRoot() {
		return root;
	}
	
	/**
	 * Sets the root for the current tree.
	 * @param root
	 */
	public void setRoot(QueryNode root) {
		this.root=root;
	}
	
	/**
	 * Gives a string representation of the queryTree using the postorder traversal
	 * @param root The root node for the query tree
	 * @return A string representation of the tree.
	 */
	public String toString(QueryNode root) {
		String ret="";
		
		// Base case: no root
		if (root==null) {
			return "";
		}
		
		ret=ret+root.getOperator().toString();
		ret=ret+toString(root.leftChild());
		ret=ret+toString(root.rightChild());
		
		return ret;
	}
}
