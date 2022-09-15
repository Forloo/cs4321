package p1.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.Aliases;
import p1.ExpressionEvaluator;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;

public class JoinOperatorTree {
	private JoinOperatorNode root;

	/**
	 * Constructor for JoinOperatorTree
	 *
	 * @param plainSelect: Gives the information for the tree
	 */
	public JoinOperatorTree(PlainSelect plainSelect, HashMap<String[], ArrayList<Expression>> exprAssignment) {

		FromItem from = plainSelect.getFromItem();
		List joins = plainSelect.getJoins();

		ArrayList<Join> allTables = new ArrayList<Join>();

		for (int i = 0; i < joins.size(); i++) {
			allTables.add((Join) joins.get(i));
		}

		// create the tree
		String[] splitted = from.toString().split(",");
		for (int i = 0; i < splitted.length; i++) {
			splitted[i] = Aliases.getTable(from.toString().split(" ")[0]);
		}
		ArrayList<Expression> conditions = null;
		Arrays.sort(splitted);

		// Loop through the hashmap to see if there is a condition
		for (String[] key : exprAssignment.keySet()) {
			// Make a copy of the key
			String[] copy = key.clone();
			Arrays.sort(copy);

			if (Arrays.equals(splitted, copy)) {
				// Assign to this node a list of conditions
				conditions = exprAssignment.get(key);
			}
		}

		JoinOperatorNode left = new JoinOperatorNode(from.toString(), null, null, conditions);
		for (Join table : allTables) {
			// make the expression to create JoinOperatorNode
			String[] splitted2 = table.toString().split(",");
			for (int i = 0; i < splitted2.length; i++) {
				splitted2[i] = Aliases.getTable(splitted2[i].split(" ")[0]);
			}
			ArrayList<Expression> conditionstwo = null;
			// Loop through the hashmap
			for (String[] key : exprAssignment.keySet()) {
				String[] copy = key.clone();
				Arrays.sort(copy);
				if (Arrays.equals(splitted2, copy)) {
					conditionstwo = exprAssignment.get(key);
				}
			}

			JoinOperatorNode node = new JoinOperatorNode(table.toString(), null, null, conditionstwo);
			if (left == null) {
				left = node;
			} else {
				String combinedname = left.getTableName() + "," + node.getTableName();
				String[] splitted3 = combinedname.split(",");
				for (int i = 0; i < splitted3.length; i++) {
					splitted3[i] = Aliases.getTable(splitted3[i]);
				}
				ArrayList<Expression> conditionsthree = null;
				Arrays.sort(splitted3);

				for (String[] key : exprAssignment.keySet()) {
					String[] copy = key.clone();
					Arrays.sort(copy);
					if (Arrays.equals(splitted3, copy)) {
						conditionsthree = exprAssignment.get(key);
					}
				}
				JoinOperatorNode parentNode = new JoinOperatorNode(combinedname, left, node, conditionsthree);
				left = parentNode;
			}
		}

		root = left;
	}

	/**
	 * Retrieves the root node for the tree
	 *
	 * @return A JoinOpeartorNode or null
	 */
	public JoinOperatorNode getRoot() {
		return root;
	}

	public HashMap<String, ArrayList<Tuple>> dfs(JoinOperatorNode root, DatabaseCatalog db) {

		if (root.getLeftChild() == null && root.getRightChild() == null) {
			ArrayList<Tuple> ret = new ArrayList<Tuple>();
			Tuple temp = null;
			temp = root.getLeafHelper().getNextTuple();
			ArrayList<Expression> conditions = root.getWhere();

			while (temp != null) {
				if (conditions != null) {
					boolean allTrue = true;
					for (int j = 0; j < conditions.size(); j++) {
						ExpressionEvaluator expr2 = new ExpressionEvaluator(temp,
								db.getSchema().get(root.getTableName()));
						Expression value = conditions.get(j);
						value.accept(expr2);
						allTrue = allTrue && (Boolean.parseBoolean(expr2.getValue()));
					}
					if (allTrue) {
						ret.add(temp);
					}
					temp = root.getLeafHelper().getNextTuple();
				} else {
					ret.add(temp);
					temp = root.getLeafHelper().getNextTuple();
				}
			}

			// Always reset the idx
			root.getLeafHelper().reset();
			// Get the string representation of this tuple
			ArrayList<String> schema = db.getSchema().get(root.getTableName());
			String prefix = "";
			String str = "";
			for (int i = 0; i < schema.size(); i++) {

				if (i == schema.size() - 1) {
					str = str + prefix + schema.get(i);
				} else {
					str = str + prefix + schema.get(i) + ",";
				}
			}
			HashMap<String, ArrayList<Tuple>> tbl = new HashMap<String, ArrayList<Tuple>>();
			tbl.put(str, ret);
			return tbl;

		}

		HashMap<String, ArrayList<Tuple>> left = null;
		if (root.getLeftChild() != null) {
			left = dfs(root.getLeftChild(), db);
		}

		HashMap<String, ArrayList<Tuple>> ret = new HashMap<String, ArrayList<Tuple>>();
		HashMap<String, ArrayList<Tuple>> right = null;

		if (root.getRightChild() != null) {
			right = dfs(root.getRightChild(), db);
		}

		// Iterate to get the one arraylist
		ArrayList<Tuple> leftList = null;
		String leftName = null;
		for (String key : left.keySet()) {
			leftName = key;
			leftList = left.get(key);
		}

		// Do the same for right
		ArrayList<Tuple> rightList = null;
		String rightName = null;
		for (String key : right.keySet()) {
			rightName = key;
			rightList = right.get(key);
		}

		// New name
		String finalName = leftName + "," + rightName;
		ArrayList<Tuple> finalList = new ArrayList<Tuple>();
		for (int i = 0; i < leftList.size(); i++) {
			Tuple curr = leftList.get(i);
			for (int j = 0; j < rightList.size(); j++) {
				Tuple curr2 = rightList.get(j);
				// Need to merge both of the tuples into one.
				String bothTuple = curr.toString() + "," + curr2.toString();
				Tuple element = new Tuple(bothTuple);
				// After merging the two new tuples into one check where conditions if they
				// exist
				ArrayList<Expression> joinedConditions = root.getWhere();
				// Get the new schema from our string
				if (joinedConditions != null) {
					ArrayList<String> schemaInput = new ArrayList<String>();
					String[] splittedSchema = finalName.split(",");
					for (int l = 0; l < splittedSchema.length; l++) {
						schemaInput.add(splittedSchema[l]);
					}
					// Loop through all the conditions
					boolean allTrue = true;
					for (int m = 0; m < joinedConditions.size(); m++) {
						Expression current = joinedConditions.get(m);
//						ExpressionEvaluator evaltwo = new ExpressionEvaluator(element, root.getTableName());
						ExpressionEvaluator evaltwo = new ExpressionEvaluator(element, schemaInput);
						current.accept(evaltwo);
						allTrue = allTrue && (Boolean.parseBoolean(evaltwo.getValue()));
					}
					if (allTrue) {
						finalList.add(element);
					}
				} else {
					finalList.add(element);
				}

			}
		}

		ret.put(finalName, finalList);

		return ret;

	}

	/**
	 * Iterate through the node using a depth first search model.
	 *
	 * @param root
	 * @return
	 */
	public JoinOperatorNode dfs(JoinOperatorNode root) {
		if (root.getLeftChild() == null && root.getRightChild() == null) {
			ArrayList<Expression> ret = root.getWhere();
			return root;
		}

		if (root.getLeftChild() != null) {
			dfs(root.getLeftChild());
		}

		if (root.getRightChild() != null) {
			dfs(root.getRightChild());
		}

		ArrayList<Expression> ret2 = root.getWhere();

		return root;

	}
}
