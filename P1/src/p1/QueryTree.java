package p1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import p1.databaseCatalog.DatabaseCatalog;
import p1.operator.ProjectOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;

public class QueryTree {

	// The root node for queryTree
	private TreeNode root;

	/**
	 * Constructor for queryTree
	 */
	public QueryTree() {
		root = null;
	}

	/**
	 * Builds an operator tree
	 *
	 * @param plainSelect: Gives the body of a query and information on building
	 *                     queryTree.
	 * @param db:          Information on where to find files and appropriate
	 *                     operators.
	 * @return A TreeNode for the root of the tree.
	 */
	public TreeNode buildTree(PlainSelect plainSelect, DatabaseCatalog db) {
		// Get the list of columns
		List allColumns = plainSelect.getSelectItems();
		// The from information for the class
		FromItem from = plainSelect.getFromItem();
		// Information from the where clause
		Expression where = plainSelect.getWhere();

		// Base case is when the operator is the scan operator
		// This happens when the query has allColumns as * and there is no where clause
		if (where == null && (allColumns.get(0) instanceof AllColumns)) {
			ScanOperator op = new ScanOperator(from.toString());
			// Make the tree node
			TreeNode leaf = new TreeNode(op, null, null);
			return leaf;
		}

		TreeNode root = null;
		// Projection is the first priority node
		if (!(allColumns.get(0) instanceof AllColumns)) {
			// Keep this as a scan operator for now and after the other
			// operators are done then change them.
			ProjectOperator op = new ProjectOperator(plainSelect, from.toString());
			// Change the select items to be the *
			List<SelectItem> all = new ArrayList<SelectItem>();
			AllColumns allCols = new AllColumns();
			all.add(allCols);
			plainSelect.setSelectItems(all);
			plainSelect.setFromItem(from);
			plainSelect.setWhere(where);
			// Make the tree node for this
			root = new TreeNode(op, null, null);
			root.setLeftChild(root, buildTree(plainSelect, db));
		}
		// Next priority match node is selection so if there is a where clause
		else if (where != null) {
			// Keep the scan operator here for now and change after the other
			// operators are done.
			SelectOperator op = new SelectOperator(plainSelect, from.toString());
			// Change the where clause to be null now
			plainSelect.setWhere(null);
			plainSelect.setSelectItems(allColumns);
			plainSelect.setFromItem(from);
			// Make the tree node for this
			root = new TreeNode(op, null, null);
			root.setLeftChild(root, buildTree(plainSelect, db));
		}

		return root;
	}

	/**
	 * Sets the root of the tree
	 *
	 * @param root: The root of the tree
	 */
	public void setRoot(TreeNode root) {
		this.root = root;
	}

	/**
	 * Gives the root of the tree
	 *
	 * @return Root node for queryTree.
	 */
	public TreeNode getRoot() {
		return root;
	}

	/**
	 * Gives a string representation of the queryTree using postorder traversal.
	 *
	 * @param root The root node for the queryTree
	 * @return A string representation of the queryTree.
	 */
	public String toString(TreeNode root) {

		String ret = "";
		// Base case: No root
		if (root == null) {
			return "";
		}

		// Add the current node to the string
		ret += root.toString(root);
		ret += toString(root.leftChild());
		ret += toString(root.rightChild());

		return ret;

	}

}
