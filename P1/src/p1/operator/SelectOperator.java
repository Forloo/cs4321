package p1.operator;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import p1.io.BinaryTupleWriter;
import p1.util.ExpressionEvaluator;
import p1.util.Tuple;

/**
 * This operator selects rows based on a where condition. Add the tuple to the
 * output if ExpressionVisitor determines that the condition is true, and skip
 * the tuple if not.
 */
public class SelectOperator extends Operator {
	// The child operator, scanning all rows.
	private Operator scanObj;
	// The expression to check rows on.
	private ArrayList<Expression> where;
	// The unionfind expression if any of them exist for the current table.
	HashMap<String, ArrayList<Integer>> ufRestraints;

	/**
	 * Determines selection conditions and rows.
	 *
	 * @param op the child scan operator.
	 * @param ex the expression to select tuples from.
	 */
	public SelectOperator(Operator op, ArrayList<Expression> ex, HashMap<String, ArrayList<Integer>> ufRestraints) {
		where = ex;
		scanObj = op;
		this.ufRestraints=ufRestraints;
	}

	/**
	 * Retrieves the next tuples matching the selection condition. If there is no
	 * next tuple then null is returned.
	 *
	 * @return the selected tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		while (true) {
			Tuple nextTuple = scanObj.getNextTuple();
			if (nextTuple == null) {
				return null;
			}
			
			Boolean allTrue= true;
			// Convert this function to handle the arraylist
			for(int i=0;i<where.size();i++) {
				ExpressionEvaluator exprObj2= new ExpressionEvaluator(nextTuple,scanObj.getSchema());
				Expression curr = where.get(i);
				curr.accept(exprObj2);
				Boolean result= Boolean.parseBoolean(exprObj2.getValue());
				allTrue=allTrue && result;
			}
			
			// Loop through all of the extra union find constraints
			for(String key: ufRestraints.keySet()) {
				// The key value must be here since we check the conditions before adding to our list
				int schema_location= this.getSchema().indexOf(key);
				// Given the schema location here we can then findt the value in the tuple that we are given
				int tuple_value= Integer.parseInt(nextTuple.getTuple().get(schema_location));
				Integer min_value= ufRestraints.get(key).get(0);
				Integer max_value = ufRestraints.get(key).get(1);
				if(tuple_value>=min_value && tuple_value<=max_value) {
					allTrue=allTrue && true;
				}
				else {
					// Violates the condition
					allTrue= allTrue && false;
				}
			}
			
			if (allTrue) {
				return nextTuple;
			}
			
//			ExpressionEvaluator exprObj2 = new ExpressionEvaluator(nextTuple, scanObj.getSchema());
//			where.accept(exprObj2);
//			if (Boolean.parseBoolean(exprObj2.getValue())) {
//				return nextTuple;
//			}
		}
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		scanObj.reset();
	}

	/**
	 * Resets the Operator to the ith tuple.
	 *
	 * @param idx the index to reset the Operator to
	 */
	public void reset(int idx) {
	}

	/**
	 * Gets the column names corresponding to the tuples.
	 *
	 * @return a list of all column names for the scan table.
	 */
	public ArrayList<String> getSchema() {
		return scanObj.getSchema();
	}

	/**
	 * Gets the table name.
	 *
	 * @return the table name.
	 */
	public String getTable() {
		return scanObj.getTable();
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
			BinaryTupleWriter out = new BinaryTupleWriter(outputFile);
			while (nextTuple != null) {
				out.writeTuple(nextTuple);
				nextTuple = getNextTuple();
			}
			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}

	/**
	 * Gets the string to print for the physical plan
	 * 
	 * @param level the level of the operator
	 * @return the physical plan in string form
	 */
	public String toString(int level) {
		return "-".repeat(level) + "Select[" + where.toString() + "]\n" + scanObj.toString(level + 1);
	}

}