package p1.operator;

import p1.Tuple;
import p1.QueryPlan2;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class SortOperator extends Operator {
	
	private Operator child;
	
	public SortOperator(PlainSelect ps, String fromTable) {
		child = QueryPlan2.getOperator();
	}
	
	public void sort() {
		Collections.sort(null);
	} 
	
	public class compareTuples implements Comparator<Tuple> {

		@Override
		public int compare(Tuple o1, Tuple o2) {
			// TODO Auto-generated method stub
			return 0;
		}
	}

	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return null;
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
