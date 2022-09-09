package p1.operator;

import java.io.PrintWriter;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.ExpressionEvaluator;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;

public class SelectOperator extends Operator {
	private ScanOperator scanObj;
	private ArrayList<String> schema;
	private Expression where;

	public SelectOperator(PlainSelect ps, String fromTable) {
		System.out.println(ps.getWhere());
		where = ps.getWhere();
		schema = DatabaseCatalog.getInstance().getSchema().get(fromTable);
		scanObj = new ScanOperator(fromTable);
	}

	/**
	 * Retrieves the next tuples matching the selection condition. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		Tuple nextTuple = scanObj.getNextTuple();
		if (nextTuple == null) {
			return null;
		}

		ExpressionEvaluator exprObj2 = new ExpressionEvaluator(nextTuple, schema);
		where.accept(exprObj2);
		if (Boolean.parseBoolean(exprObj2.getValue())) {
			return nextTuple;
		} else {
			return getNextTuple();
		}

	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		scanObj.reset();
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	public void dump() {
		Tuple nextTuple = getNextTuple();
		while (nextTuple != null) {
			System.out.println(nextTuple.toString());
			nextTuple = getNextTuple();
		}
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
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
}