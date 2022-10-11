package p1.operator;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import p1.util.ExpressionEvaluator;
import p1.util.Tuple;

public class BNLJOperator extends Operator{

	// The left operator for bnlj 
	private Operator left;
	// The right operator for bnlj 
	private Operator right;
	// The schema for the result table
	private ArrayList<String> schema;
	// The list of expression for bnlj 
	private ArrayList<Expression> where;
	//Keep track of where we are in the tuple for the left table of the joined column
	private Tuple currTuple;
	// The current right tuple value
	private Tuple rightTuple;
	// A string denoting the list of tables that we need for executing the given join
	private String tables;
	
	// The number of tuples that we scan into a block in total
	private int tuplePerScan;
	// The number of bytes for a page.
	public static final int pageSize=4096;
	// The Arraylist of tuples containing the elements in the outer block
	private ArrayList<Tuple> outerBlock;
	private int outerPos;
	private int blockNumber;
	private boolean lastBlock;
	
	/**
	 * The constructor for bnlj
	 * @param tables The string delimited by commas that provided the tables needed for the join
	 * @param left The left child of the bnlj 
	 * @param right The right child of the bnlj
	 * @param exp The list of expressions imposed on the result tuples of the bnlj
	 * @param bufferSize The number of pages that will be used for the outer loops blocks
	 */
	public BNLJOperator(String tables,Operator left, Operator right, ArrayList<Expression> exp, int bufferSize) {
		this.tables=tables;
		this.left=left;
		this.right=right;
		where=exp;
		
		ArrayList<String> schema2= new ArrayList<String>();
		schema2.addAll(left.getSchema());
		schema2.addAll(right.getSchema());
		schema=schema2;
		blockNumber=0;
		
		// The size of the left table is how long the schema is 
		int leftTuplelength= left.getSchema().size();
		// Subtract 8 since each page has metadata that takes up 8 bytes
		tuplePerScan= bufferSize*((pageSize-8)/(4*leftTuplelength));
		
		// Need to only get the block if the inner table returns some tuple to us
		rightTuple=right.getNextTuple();
//		System.out.println(rightTuple);
		if (rightTuple!=null) {
			outerBlock=this.getBlock();
			outerPos=0;
		}
	}
	
	
	/**
	 * Retrieves the next tuple for bnlj
	 */
	public Tuple getNextTuple() {
		// Given each block read the outer table in its entirety 
		// over the inner table
		
		while(true) {
			// Check if the index for the outer block is in range
			if (outerPos>=outerBlock.size()) {
				// Get the next right tuple if that value is not null
				// then that means that we need to reset the outer tuple 
				// back to the beginning 
				rightTuple=right.getNextTuple();
				// If the right tuple is null then it means that we are done
				// with this current outer block
				if (rightTuple==null) {
					if (lastBlock) {
						// If this is the last block then there is no next tuple
						return null;
					}
					// Otherwise if it not the last block get the next block
					outerBlock=this.getBlock();
					
					// If we reset the outerblock then we need to reset the outerpos 
					outerPos=0;
					// Getting the next block then we need to reset the right operator
					right.reset();
					rightTuple=right.getNextTuple();
				}
				// If the righttuple is not null then we still have other tuples
				// on the outer table to iterate through
				else {
					outerPos=0;
				}
			}
			
			// The left tuple value here is the one in the arraylist for the outerblock
			Tuple leftSide= outerBlock.get(outerPos);
			// The value of the right tuple is just hte current right tuple
			Tuple currRight= rightTuple;
			// Make them into one larger tuple
			ArrayList<String> together= new ArrayList<String>();
			together.addAll(leftSide.getTuple());
			together.addAll(currRight.getTuple());
			
			// Make the new tuple
			Tuple combined= new Tuple(together);
			outerPos=outerPos+1;
			
			if (where==null) {
				return combined;
			}
			else {
				ExpressionEvaluator eval = new ExpressionEvaluator(combined, schema);
				// There must be at least one expression if we enter this loop
				boolean allTrue = true;
				for (int i = 0; i < this.getWhere().size(); i++) {
					this.getWhere().get(i).accept(eval);
					allTrue = allTrue && (Boolean.parseBoolean(eval.getValue()));
				}
				if (allTrue) {
					return combined;
				}
			}
			
		}
	}
	
	/**
	 * Retrieves the conditions for the join
	 * @return An arraylist of all the expression for bnlj
	 */
	public ArrayList<Expression> getWhere(){
		return where;
	}

	@Override
	public void reset() {
		// Reset the outer block of the loop which is just resetting the left child
		// Reset the left operator
		left.reset();
		right.reset();
		outerPos=0;
		blockNumber=0;
		currTuple=null;
		rightTuple=right.getNextTuple();
		if (rightTuple!=null) {
			outerBlock=this.getBlock();
			outerPos=0;
		}
	}

	@Override
	public void reset(int idx) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ArrayList<String> getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void dump() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dump(String outputFile) {
		// TODO Auto-generated method stub
		
	}
	/**
	 * Retrives the current block number of the outer block that we are currently
	 * @return An integer telling us the block we are on for the outer block.
	 */
	public int getBlockNumber() {
		return blockNumber;
	}
	
	/**
	 * A method to get the outer block for bnlj
	 * @return A list of tuples for the outer block.
	 */
	private ArrayList<Tuple> getBlock(){
		blockNumber=blockNumber+1;
		int tuplesToScan=tuplePerScan;
		ArrayList<Tuple> outer= new ArrayList<Tuple>();
		
		// Check if there is a placeholder element for the last tuple that we scanned on
		// left child but there was no room left in the last buffer
		if (currTuple!=null) {
			tuplesToScan=tuplesToScan-1;
			outer.add(currTuple);
		}
		// Get the next tuple for the left table
		Tuple leftTuple=left.getNextTuple();
		while (tuplesToScan>0 && leftTuple!=null) {
			tuplesToScan=tuplesToScan-1;
			outer.add(leftTuple);
			leftTuple=left.getNextTuple();
		}
		
		// Left tuple not null then keep track of this tuple as the next start tuple
		if (leftTuple!=null) {
			currTuple=leftTuple;
		}
		
		// If there are remaining tuples when we get here then that means that 
		// this must be the last block
		if (tuplesToScan>0) {
			lastBlock=true;
		}
		
		return outer;
	}

}
