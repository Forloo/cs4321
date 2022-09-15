package p1.operator;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;

public class ProjectOperator extends Operator {
	private Operator child = null;
	private ArrayList<String> schema;
	private ArrayList<SelectItem> cols = new ArrayList<SelectItem>();

	public ProjectOperator(PlainSelect ps, String fromTable) {
		schema = DatabaseCatalog.getInstance().getSchema().get(fromTable);

		if (ps.getWhere() != null) { // determine if child is selectoperator or scanoperator
			child = new SelectOperator(ps, fromTable);
		} else {
			child = new ScanOperator(fromTable);
		}
		List selectItems = ps.getSelectItems(); // specific columns
		for (int i = 0; i < selectItems.size(); i++) {
			cols.add((SelectItem) selectItems.get(i));
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

		for (SelectItem i : cols) {
			String[] colName = i.toString().split("\\.");
			if (colName[colName.length - 1].equals("*")) {
				projection.add(nextTuple.toString());
			} else {
				int idx = schema.indexOf(colName[colName.length - 1]);
				projection.add(nextTuple.getTuple().get(idx));
			}
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
