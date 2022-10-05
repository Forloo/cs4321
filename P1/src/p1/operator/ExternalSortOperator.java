package p1.operator;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import p1.io.BinaryTupleWriter;
import p1.io.BinaryTupleReader;
import p1.util.Tuple;
import p1.util.PhysicalPlanBuilder;

/**
 * Physical external sort operator
 */
public class ExternalSortOperator extends Operator {
	
	private Operator child = null;
	private ArrayList<String> schema = new ArrayList<String>();
	private BinaryTupleReader reader = null;
	private int bufferPages;
	
	public ExternalSortOperator(Operator op, List orders) {
		child = op;
		schema = op.getSchema(); 
		bufferPages = 0;
		
	}
	
	/**
	 * Create number of runs, sort each run  
	 */
	public void sort() {
		
	}
	
	/**
	 * Merge the runs 
	 */
	public void merge() {
		int tuplesPerPage = 4096 / schema.size() / 4;
		int totalTuples = tuplesPerPage * bufferPages;

	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
		if (reader == null) return null;
	    Tuple tp = reader.nextTuple();
		   return tp;
	}

	/**
	 * Tells the operator to reset its state and start reading its output again
	 * from the beginning
	 */
	@Override
	public void reset() {
		if (reader == null) return;
        reader.reset();		
	}

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the scan table.
	 */
	@Override
	public ArrayList<String> getSchema() {
		// TODO Auto-generated method stub
		return schema;
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

	
