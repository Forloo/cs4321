package p1.operator;

import p1.Tuple;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import p1.databaseCatalog.DatabaseCatalog;


public class ProjectOperator extends Operator {
	private Operator child = null;
	private ArrayList<String> schema;
	private boolean selectAll = false;
	private ArrayList<SelectItem> cols = new ArrayList<String>();

	public ProjectOperator(PlainSelect ps, String fromTable) {
		schema = DatabaseCatalog.getInstance().getSchema().get(fromTable);
		
		if (ps.getWhere() != null) { // determine if child is selectoperator or scanoperator 
			child = new SelectOperator(fromTable);
		} else {
			child = new ScanOperator(fromTable);
		}
		
		selectItems = plainSelect.getSelectItems(); //specific columns 
		if (selectItems.get(0).toString() == "*") { //don't create projectoperator 
			selectAll = true;
		} else {
			for (int i = 0; i < selectItems.size(); i++) {
				cols.add(selectItems.get(i).toString(););
			} 
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
		} else {
			if (selectAll = true) {
				return nextTuple;
			}
		}
		
		List<SelectItem> projection = new ArrayList<>();
		
		for (SelectItem i : cols) {
			ExpressionEvaluator exprObj = new ExpressionEvaluator(nextTuple, schema);
			i.accept(exprObj);
			projection.add(i);
		} 
		return new Tuple(projection.toString());
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
				System.out.println(nextTuple.toString());
				nextTuple = getNextTuple();
			}
			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}
}

