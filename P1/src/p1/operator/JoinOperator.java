package p1.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import p1.io.BinaryTupleWriter;
import p1.util.Aliases;
import p1.util.ExpressionEvaluator;
import p1.util.ExpressionParser;
import p1.util.Tuple;

/**
 * An operator that processes queries on multiple files and conditions.
 */
public class JoinOperator extends Operator {

	// Left child operator
	private Operator left;
	// Right child operator
	private Operator right;
	// Tree representing the join operator
	private JoinOperatorTree tree;
	// A list of tuples containing the final results.
	private ArrayList<Tuple> results;
	// Index for which tuple we are on
	private int idx;
	// The schema for the results query table
	private ArrayList<String> schema;
	// The where condition. Only used to call accept.
	private Expression where;
	// The left tuple to join with.
	private Tuple leftTuple;

	/**
	 * Creates a JoinOperatorTree.
	 *
	 * @param left  the left child operator
	 * @param right the right child operator
	 * @param exp   the expression to join the two operators on
	 */
	public JoinOperator(Operator left, Operator right, Expression exp) {
		this.left = left;
		this.right = right;

		// Split the where expression
		// DO WE STILL NEED THIS?
		Expression whereClause = exp;
		ExpressionParser parse = new ExpressionParser(whereClause);
		where = exp;
		where.accept(parse);
		HashMap<String[], ArrayList<Expression>> expressionInfoAliases = parse.getTablesNeeded();
		HashMap<String[], ArrayList<Expression>> expressionInfo = new HashMap<String[], ArrayList<Expression>>();

		// Pad tables needed for expressions: if join A, B, C and expression is A.C1 =
		// C.C2, key will be [A, B, C] not [A, C]
		for (Map.Entry<String[], ArrayList<Expression>> set : expressionInfoAliases.entrySet()) {
			if (set.getKey().length > 1) {
				ArrayList<String> key = new ArrayList<String>();
				int idx = -1;
				for (String s : set.getKey()) {
					int keyIdx = Aliases.getOnlyAliases().indexOf(s);
					if (keyIdx > idx) {
						idx = keyIdx;
					}
				}
				for (int i = 0; i <= idx; i++) {
					String alias = Aliases.getOnlyAliases().get(i);
					key.add(alias);
				}

				expressionInfo.put(key.toArray(new String[key.size()]), set.getValue());
			} else {
				expressionInfo.put(set.getKey(), set.getValue());
			}
		}

		tree = new JoinOperatorTree(plainSelect, expressionInfo);

//		HashMap<String, ArrayList<Tuple>> tbl = tree.dfs(tree.getRoot(), DatabaseCatalog.getInstance());
//		for (String key : tbl.keySet()) {
//			results = tbl.get(key);
//			ArrayList<String> temp = new ArrayList<String>();
//			String[] arr = key.split(",");
//			for (int i = 0; i < arr.length; i++) {
//				temp.add(arr[i]);
//			}
//			schema = temp;
//		}
		schema = left.getSchema();
		schema.addAll(right.getSchema());
		idx = 0;
		leftTuple = left.getNextTuple();
	}

	/**
	 * Retrieves the where condition.
	 *
	 * @return An expression or null.
	 */
	public Expression getWhere() {
		return where;
	}

	/**
	 * Retrieves the schema information.
	 *
	 * @return An arraylist representing the schema.
	 */
	public ArrayList<String> getSchema() {
		return schema;
	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
//		if (idx == results.size()) {
//			return null;
//		}
//		return new Tuple(results.get(idx++).toString());
		if (leftTuple == null) { // no more tuples to join
			return null;
		}
		Tuple rightTuple = right.getNextTuple();
		if (rightTuple == null) {
			right.reset();
			rightTuple = right.getNextTuple();
			leftTuple = left.getNextTuple();
		}
		if (leftTuple == null) { // no more tuples to join
			return null;
		}
		ArrayList<String> together = leftTuple.getTuple();
		together.addAll(rightTuple.getTuple());
		Tuple joinedTuple = new Tuple(together);
		if (where == null) {
			return joinedTuple;
		} else {
			ExpressionEvaluator eval = new ExpressionEvaluator(joinedTuple, schema);
			where.accept(eval);
			if (Boolean.parseBoolean(eval.getValue())) {
				return joinedTuple;
			} else {
				return getNextTuple();
			}
		}
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	@Override
	public void reset() {
		idx = 0;
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	@Override
	public void dump() {
		Tuple temp = this.getNextTuple();
		while (temp != null) {
			System.out.println(temp);
			temp = this.getNextTuple();
		}
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	@Override
	public void dump(String outputFile) {
		Tuple nextTuple = getNextTuple();
		try {
			BinaryTupleWriter out = new BinaryTupleWriter(outputFile);
			while (nextTuple != null) {
				out.writeTuple(nextTuple);
				nextTuple = getNextTuple();
			}
			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}

	/**
	 * Returns the root of the JoinOperatorTree.
	 *
	 * @return A tree representing the order of joins.
	 */
	public JoinOperatorTree getRoot() {
		return tree;
	}

}
