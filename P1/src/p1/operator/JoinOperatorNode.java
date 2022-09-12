package p1.operator;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import p1.Tuple;

public class JoinOperatorNode {
	
	// Left join child
	private JoinOperatorNode left;
	// Right join child
	private JoinOperatorNode right;
	// None leaf nodes can have a where restriction
	private Expression where=null;
	// From the return of two leaf nodes we need something to hold the list of tuples
	private ArrayList<Tuple> table= null;
	// Each leaf node will have an optional scan or select depending on whether they have some conditon on them
	private Operator leafHelper=null;
	// Give a name to the node
	private String tableName;
	
	
	public JoinOperatorNode(String tableName,JoinOperatorNode left,JoinOperatorNode right, Expression where) {
		
		// Iterating through each of the table names we make a node. There will be a left child if prev
		// field is none else it is null. The right child will always be a null field
		
		this.left=left;
		this.right=right;
		this.tableName=tableName;
		
		// Keep the table null it will be not null when something is returned from its two childrens
		
		if (this.left==null && this.right==null) {
			// Need to either make a plainselect with the right properties or 
			// instead we pass in our own where clause for each of the operators
			leafHelper=new ScanOperator(tableName);
		}
		else {
			// Set the expression for Expression if it is not null.
			where=null;
		}
	}
	
	/**
	 * Retrieves the table/joined table
	 * @return A string representing the tablename
	 */
	public String getTableName() {
		return tableName;
	}
	
	/**
	 * Retrieves the JoinOperatorNode left child
	 * @return the a JoinOperatorNode or null if there is no child
	 */
	public JoinOperatorNode getLeftChild() {
		return left;
	}
	
	/**
	 * Updates the left child
	 * @param left: new left child
	 */
	public void setLeftChild(JoinOperatorNode left){
		this.left=left;
	}
	/**
	 * Update the right child
	 * @param right: new right child
	 */
	public void setRightChild(JoinOperatorNode right){
		this.right=right;
	}
	
	/**
	 * Retrieves the JoinOperatorNode right child
	 * @return the a JoinOperatorNode or null if there is no child
	 */
	public JoinOperatorNode getRightChild() {
		return right;
	}
	
	/**
	 * Retrieves the condition for the join
	 * @return An expression else null if there is no conditions
	 */
	public Expression getWhere() {
		return where;
	}
	
	/**
	 * Retrieves the table from the two leaf node joins 
	 * @return An arraylist from joining two leaf nodes or null if this is a leaf
	 */
	public ArrayList<Tuple> getTable(){
		return table;
	}
	
	/**
	 * Retrieves the operator for a leaf node
	 * @return Null if this is not a leaf node else returns an operator
	 */
	public Operator getLeafHelper() {
		return leafHelper;
	}
	
	
	
	
	
}
