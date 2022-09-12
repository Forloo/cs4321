package p1.operator;

import java.util.ArrayList;

import net.sf.jsqlparser.statement.select.Join;
import p1.Tuple;

/**
 * An operator that processes queries on multiple files and conditions.
 */
public class JoinOperator extends Operator{
	
	private Operator leftOperator=null;
	private Operator rightOperator=null;

	public JoinOperator(PlainSelect plainSelect, String fromTable) {
		
		// Using the plainselect get the table from the from section. Get the rest of the tables from the join
		// portion of the clause.
		// Queryplan will only enter this if we have more than one table so lets make a list of all the tables that we have
		ArrayList<Join> tableList= new ArrayList<Join>();
		
		// Get the join item
		
	}
	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	@Override
	public void dump() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	@Override
	public void dump(String outputFile) {
		// TODO Auto-generated method stub
		
	}

}
