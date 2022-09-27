package p1.util;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class QueryTreePlan {
	
	// The query plan will be represented by a tree.
	private QueryTree tree;
	
	/**
	 * Constructor for the queryplan tree built using the query and the databasecatalog.
	 * @param query
	 * @param db
	 */
	public QueryTreePlan(Statement query, DatabaseCatalog db) {
		// Dp this later the important part will be translating the code from the nodes to this structure
		Select select= (Select) query;
		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
		QueryTree treeValue = new QueryTree();
		QueryNode root = treeValue.buildTree(plainSelect, db);
		treeValue.setRoot(root);
	}
	
	/**
	 * Constructor for the queryplan tree built using the visitor plan from the logical plan.
	 * @param treeInput A tree that will become the tree for our query plan.
	 */
	public QueryTreePlan(QueryTree treeInput) {
		// After using the visitor pattern to build up the tree we can just set it as our tree after
		// doing the conversion
		tree=treeInput;
	}
	
	/**
	 * Retrieves the query in the form of a tree
	 * @return A queryTree 
	 */
	public QueryTree getTree() {
		return tree;
	}
	
	
}
