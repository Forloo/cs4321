package p1.operator;

import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * This operator sorts tuples according to order by conditions.
 */
public class SortOperator extends Operator {
	
	private Operator child = null;
	private ArrayList<String> schema = new ArrayList<String>();
	List<Column> orderBy = new ArrayList<>(); 
	int idx = 0;
	List<Tuple> tupleData = new ArrayList<Tuple>();
	private ArrayList<String> cols = new ArrayList<String>();
	
	/**
	 * Determines child and the order by which to sort elements.  
	 *
	 * @param ps        the query.
	 * @param fromTable the table to sort. 
	 */ 
	public SortOperator(PlainSelect ps, String fromTable) {
		schema = DatabaseCatalog.getInstance().getSchema().get(fromTable);

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
		
		orderBy = ps.getOrderByElements(); 
		
		Tuple currTuple = null;
		
		while (child.getNextTuple() != null) {
			tupleData.add(currTuple);
			currTuple = child.getNextTuple();
		} 
		
		for (int i = 0; i < tupleData.size(); i++) { 
			cols.add(tupleData.get(i).toString()); 
			
			while (cols != null) { 
				if (!cols.contains(orderBy.get(i).toString())) {
					cols.remove(orderBy.get(i).toString());
				}
			}
		} 
				
		Collections.sort(tupleData, new compareTuples());
	} 
	
	/**
	 * Compares elements in order specified by orderby, followed by remaining columns. 
	 */
	public class compareTuples implements Comparator<Tuple> {
	
		@Override
		public int compare(Tuple o1, Tuple o2) {

			if (orderBy != null) {
				for (int i=0; i < orderBy.size(); i++) { 
					int col = Integer.valueOf(orderBy.get(i).toString());
					
					int o1_int = Integer.valueOf(o1.getTuple().get(col));
					int o2_int = Integer.valueOf(o2.getTuple().get(col));
					
					if (o1_int < o2_int) {
						return -1;
					} 
					else if (o1_int > o2_int) {
						return 1;
					} 
				} 
			} 
			
			for (int i = 0; i < cols.size(); i++) {
				int col = Integer.valueOf(cols.get(i));
				
				int o1_int = Integer.valueOf(o1.getTuple().get(col));
				int o2_int = Integer.valueOf(o1.getTuple().get(col));
				
				if (o1_int < o2_int) {
					return -1;
				} 
				else if (o1_int > o2_int) {
					return 1;
				}
			}
			
			return 0;
		}
	}

	/**
	 * Retrieves the next tuples matching the selection condition. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple currTuple = null;
		if (idx < tupleData.size()) {
			currTuple = tupleData.get(idx);
			idx++;
		}
		return currTuple;
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		idx = 0;
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
