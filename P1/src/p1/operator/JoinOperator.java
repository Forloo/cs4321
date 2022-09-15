package p1.operator;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.Aliases;
import p1.ExpressionParser;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;

/**
 * An operator that processes queries on multiple files and conditions.
 */
public class JoinOperator extends Operator {

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

	public JoinOperator(PlainSelect plainSelect, String fromTable, DatabaseCatalog db) {
		// Split the where expression
		Expression whereClause = plainSelect.getWhere();
		ExpressionParser parse = new ExpressionParser(whereClause);
		where = plainSelect.getWhere();
		where.accept(parse);
		HashMap<String[], ArrayList<Expression>> expressionInfoAliases = parse.getTablesNeeded();
		HashMap<String[], ArrayList<Expression>> expressionInfo = new HashMap<String[], ArrayList<Expression>>();
		for (Map.Entry<String[], ArrayList<Expression>> set : expressionInfoAliases.entrySet()) {

			// Printing all elements of a Map
			System.out.println(set.getKey()[0] + " = " + set.getValue());

			String[] key = new String[set.getKey().length];
			for (int i = 0; i < key.length; i++) {
				key[i] = Aliases.getTable(set.getKey()[i]);
			}

			expressionInfo.put(key, set.getValue());
		}
		for (Map.Entry<String[], ArrayList<Expression>> set : expressionInfo.entrySet()) {
			System.out.println(set.getKey()[0] + " = " + set.getValue());
		}

		tree = new JoinOperatorTree(plainSelect, expressionInfo);

		HashMap<String, ArrayList<Tuple>> tbl = tree.dfs(tree.getRoot(), db);
		for (String key : tbl.keySet()) {
			results = tbl.get(key);
			ArrayList<String> temp = new ArrayList<String>();
			String[] arr = key.split(",");
			for (int i = 0; i < arr.length; i++) {
				temp.add(arr[i]);
			}
			System.out.println(temp);
			schema = temp;
		}
		idx = 0;
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
		if (idx == results.size()) {
			return null;
		}
		return new Tuple(results.get(idx++).toString());
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
			PrintWriter out = new PrintWriter(outputFile);

			while (nextTuple != null) {
				out.println(nextTuple.toString());
				nextTuple = getNextTuple();
			}
			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}

	public JoinOperatorTree getRoot() {
		return tree;
	}

}
