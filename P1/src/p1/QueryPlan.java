package p1;
import p1.TreeNode;
import p1.databaseCatalog.DatabaseCatalog;
import p1.QueryTree;
import p1.Tuple;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
public class QueryPlan {
	// A queryTree representing the query plan.
	private QueryTree plan;
	
	/**
	 * The constructor for a QueryPlan object
	 * @param query
	 * @param db
	 */
	public QueryPlan(Statement query,DatabaseCatalog db) {
		
		// Get the select statement
		Select select= (Select) query;
		// Convert to plainSelect so we can get the body information
		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
		// Construct tree
		plan=new QueryTree();
		// Set the root node of the tree.
		TreeNode root=plan.buildTree(plainSelect, db);
		plan.setRoot(root);
}
	
	/**
	 * Returns a string representation of queryPlan.
	 */
	public String toString() {
		// Get the root for our current tree
		TreeNode root=plan.getRoot();
		return plan.toString(root);
	}
	
}
