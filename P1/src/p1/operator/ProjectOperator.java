package p1.operator;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;

/**
 * An operator that returns selected columns based on projection requirements.
 */
public class ProjectOperator extends Operator {
	// The child operator.
	private Operator child = null;
	// The column names.
	private ArrayList<String> schema = new ArrayList<String>();
	// The columns of the rows to return.
	private ArrayList<String> cols = new ArrayList<String>();
	// The index of the row we are currently checking/returning.
	int idx = 0;

	/**
	 * Initializes the variables above.
	 *
	 * @param ps        the query
	 * @param fromTable the initial table to retrieve rows from
	 */
	public ProjectOperator(PlainSelect ps, String fromTable) {
		ArrayList<String> fromSchema = DatabaseCatalog.getInstance().getSchema().get(fromTable);
		String alias = ps.getFromItem().getAlias() == null ? fromTable : ps.getFromItem().getAlias();
		for (String colName : fromSchema) {
			schema.add(alias + "." + colName);
		}

		if (ps.getJoins() != null) { // determine if child is join, select, or scan
			JoinOperator op = new JoinOperator(ps, fromTable);
			child = op;
			for (Object join : ps.getJoins()) {
				String[] joinTable = join.toString().split(" ");
				String joinTableName = joinTable[0];
				ArrayList<String> colSchema = DatabaseCatalog.getInstance().getSchema().get(joinTableName);
				String j = ((Join) join).getRightItem().toString();
				// Add alias to column name to distinguish for self joins
				for (String colName : colSchema) {
					schema.add(j.substring(j.lastIndexOf(" ") + 1) + "." + colName);
				}
			}
		} else if (ps.getWhere() != null) {
			child = new SelectOperator(ps, fromTable);
		} else {
			child = new ScanOperator(fromTable);
		}
		List selectItems = ps.getSelectItems(); // get specific select columns
		for (int i = 0; i < selectItems.size(); i++) {
			cols.add(selectItems.get(i).toString());
		}
	}

	/**
	 * Retrieves the next tuples matching the selection condition. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		Tuple nextTuple = child.getNextTuple();

		if (nextTuple == null) {
			return null;
		}

		ArrayList<String> projection = new ArrayList<>();

		for (String i : cols) {
			int idx = schema.indexOf(i);
			projection.add(nextTuple.getTuple().get(idx));
		}
		return new Tuple(String.join(",", projection));
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		child.reset();
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
