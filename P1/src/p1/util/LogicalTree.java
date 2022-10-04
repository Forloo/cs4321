package p1.util;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalTree {

	private LogicalNode root;

	public LogicalTree() {
		root = null;
	}

	public LogicalNode buildTree(PlainSelect plainSelect) {

		// The base case will be when we either hit a scan node or when we hit a
		// select node

		List allColumns = plainSelect.getSelectItems();

		FromItem from = plainSelect.getFromItem();
		// Extract aliases
		Aliases.getInstance(plainSelect);
		String fromTable = from.toString();
		if (from.getAlias() != null) {
			fromTable = Aliases.getTable(from.getAlias());
		}

		Expression where = plainSelect.getWhere();

		Distinct distinct = plainSelect.getDistinct();

		List orderElement = plainSelect.getOrderByElements();

		List joinElement = plainSelect.getJoins();

		return null;
	}

	public void setRoot(LogicalNode root) {
		this.root = root;
	}

	public LogicalNode getRoot() {
		return root;
	}

	public void dfs(LogicalNode root) {

		if (root.leftChild() == null && root.rightChild() == null) {
			System.out.println(root.toString());
			return;
		}

		if (root.leftChild() != null) {
			dfs(root.leftChild());
		}

		if (root.rightChild() != null) {
			dfs(root.rightChild());
		}

		System.out.println(root);
	}

}
