package p1.util;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalJoin;
import p1.logicaloperator.LogicalProject;
import p1.logicaloperator.LogicalScan;
import p1.logicaloperator.LogicalSort;
import p1.logicaloperator.LogicalUnique;

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

		// I will make the plan assuming that for any given query only one of the joins
		// will be used
		// and not multiple nested types of joins

		// Base case is either the select or scan operator
//		System.out.println(allColumns.get(0));
		if (where == null && ((allColumns.get(0)) instanceof AllColumns) && distinct == null && orderElement == null
				&& joinElement == null) {
			LogicalScan op = new LogicalScan(plainSelect, fromTable);
			// Make it into a node
			LogicalNode leaf = new LogicalNode(op, null, null);
			return leaf;
		}

		// Other base case
		if (((allColumns.get(0)) instanceof AllColumns) && distinct == null && orderElement == null
				&& joinElement == null && where != null) {
			LogicalFilter op = new LogicalFilter(plainSelect, fromTable);
			LogicalNode leaf = new LogicalNode(op, null, null);
			return leaf;
		}

		// Join is a base case
		if (((allColumns.get(0)) instanceof AllColumns) && distinct == null && orderElement == null
				&& joinElement != null) {
			// Join can be made by going through our list of tables and making those nodes.

			// Make the join sequential so that we know we need a join node but then we do
			// not need a right child
			// for our query plan

			LogicalNode prev = null;
			Boolean used = false;
			LogicalNode last = null;
			for (int i = 0; i < joinElement.size(); i++) {
				if (!used) {
					String firstTable = fromTable;
					String otherTable = joinElement.get(i).toString();
					String combinedName = firstTable + "," + otherTable;
					LogicalJoin curr = new LogicalJoin(plainSelect, combinedName);
					used = true;
					// Make the new node object
					LogicalNode node = new LogicalNode(curr, null, null);
					prev = node;
				} else {
					// The prev is not null so that is our left list value
					String firstTable = prev.getLogicalOperator().getName();
					String otherTable = joinElement.get(i).toString();
					String combinedName = firstTable + "," + otherTable;
					LogicalJoin curr = new LogicalJoin(plainSelect, combinedName);
					// Make the new node object
					LogicalNode node = new LogicalNode(curr, prev, null);
					// Update the prev object
					prev = node;

				}
			}

			// The previous node will always have our last value
			return prev;
		}

		// If it is none of the top three then it must be either distinct, sort or a
		// projection that we need.
		if (distinct != null) {
			LogicalUnique op = new LogicalUnique(plainSelect, fromTable);
			LogicalNode root = new LogicalNode(op, null, null);
			// Insert the recursive call here

			// Trying to modify the plainSelect that we are given
			plainSelect.setDistinct(null);
			root.setLeftChild(this.buildTree(plainSelect));

			return root;
		} else if (orderElement != null) {
			LogicalSort op = new LogicalSort(plainSelect, fromTable);
			LogicalNode root = new LogicalNode(op, null, null);
			// Insert the recursive call here

			// Trying to modify the plainSelect that we are given
			plainSelect.setOrderByElements(null);
			root.setLeftChild(this.buildTree(plainSelect));
			return root;
		} else if (!((allColumns.get(0)) instanceof AllColumns)) {
			LogicalProject op = new LogicalProject(plainSelect, fromTable);
			LogicalNode root = new LogicalNode(op, null, null);
			// Insert the recursive call here

			// Trying to modify the plainSelect that we are given
			AllColumns colValue = new AllColumns();
			List items = new ArrayList<Object>();
			items.add(colValue);
			plainSelect.setSelectItems(items);
			root.setLeftChild(this.buildTree(plainSelect));

			return root;
		}

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
