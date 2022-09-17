package p1.operator;

import java.io.PrintWriter;

import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.Tuple;

/**
 * This operator reads tuples from its child and only outputs non-duplicates.
 */
public class DuplicateEliminationOperator extends Operator {

	private SortOperator child;
	private Tuple prev;
	boolean check;
<<<<<<< HEAD
	
	/**
	 * This operator reads tuples from its child and only outputs non-duplicates.
	 * 
	 */
	public DuplicateEliminationOperator (PlainSelect ps, String fromTable) {
		if (ps.getOrderByElements() == null) {
			child = new SortOperator(ps, fromTable);
		} 
		if (ps.getDistinct() != null) {
			check = true;
		} else {
			check = false;
		}
=======

	public DuplicateEliminationOperator(PlainSelect ps, String fromTable) {
		child = new SortOperator(ps, fromTable);
>>>>>>> 085e2ab2817f134f5eaabfc05c9f5ea07b85b65e
	}

	/**
	 * Retrieves the next tuple that is not a duplicate. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple next = child.getNextTuple();
<<<<<<< HEAD
		int nextint = Integer.valueOf(next.toString());
		int prevint = Integer.valueOf(prev.toString()); 
		
		if (check) {
			if (prev != null) { 
				while (next != null && nextint != prevint) { 
					next = child.getNextTuple();
				}
			}
		} 
=======
		System.out.println(prev + " " + next);
		if (next == null) {
			return null;
		}
		if (prev == null) {
			prev = next;
			return next;
		}

		if (next.toString().equals(prev.toString())) {
			return getNextTuple();
		}
>>>>>>> 085e2ab2817f134f5eaabfc05c9f5ea07b85b65e
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
