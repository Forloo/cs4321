package p1.util;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.logicaloperator.LogicalOperator;

public class LogicalPlan {
	
	// Make a tree for the logical plan
	
	// Priority for specific elements
	// 1. Distinct
	// 2. Sorting operator
	// 3. Projection 
	// 4. Join
	// 5. Selection
	// 6. Scan operator
	
	// The logicaltree representing the logical plan
	private LogicalTree plan;
	
	/**
	 * The constructor for our logicalplan
	 * @param query The input query
	 */
	public LogicalPlan(Statement query) {
		
		Select select = (Select) query;
		PlainSelect plainSelect= (PlainSelect) select.getSelectBody();
		LogicalTree tree = new LogicalTree();
		LogicalNode root=tree.buildTree(plainSelect);
		tree.setRoot(root);
		
		plan=tree;
	}
	
	/**
	 * A method that retrieves all of the operators in the tree.
	 * @param root The root node for our tree.
	 * @return An arraylist containng all of our nodes in a postorder traversal.
	 */
	public ArrayList<LogicalNode> getOperators(LogicalNode root){
		
		if (root.leftChild()==null && root.rightChild()==null) {
			ArrayList<LogicalNode> ret = new ArrayList<LogicalNode>();
			ret.add(root);
			return ret;
		}
		
		ArrayList<LogicalNode> ret=null;
		if (root.leftChild()!=null) {
			ret=getOperators(root.leftChild());
		}
		
		ret.add(root);
		
		return ret;
		
	}
	/**
	 * Retrieves the plan in the form of a tree
	 * @return A logical Tree.
	 */
	public LogicalTree getTree() {
		return plan;
	}
	/**
	 * Retrieves the root for the tree
	 * @return A logicalnode
	 */
	public LogicalNode getRoot() {
		return plan.getRoot();
	}
	/**
	 * Accepts a physicalplanbuilder object and then uses it to build the physical tree.
	 * @param pb
	 */
	public void accept(PhysicalPlanBuilder pb) {
		pb.visit(this);
	}
}
