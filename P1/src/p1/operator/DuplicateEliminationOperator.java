package p1.operator; 

import p1.Tuple;
import p1.QueryPlan;
import java.io.PrintWriter;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class DuplicateEliminationOperator extends Operator {
	
	private Operator child;
	private Tuple prev;
	boolean check;
	
	public DuplicateEliminationOperator (PlainSelect ps, String fromTable) {
		if (ps.getOrderByElements() == null) {
			child = new SortOperator(ps, fromTable);
		} 
		if (ps.getDistinct() != null) {
			check = true;
		} else {
			check = false;
		}
	}

	@Override
	public Tuple getNextTuple() {
		Tuple next = child.getNextTuple();
		int nextint = Integer.valueOf(next.toString());
		int prevint = Integer.valueOf(prev.toString()); 
		
		if (check) {
			if (prev != null) {
				while (next != null && nextint != prevint) { 
					next = child.getNextTuple();
				}
			}
		} 
		prev = next;
		return next;
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

