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
		this.ufRestraints = ufRestraints;
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

			Boolean allTrue = true;
			// Convert this function to handle the arraylist
			for (int i = 0; i < where.size(); i++) {
				ExpressionEvaluator exprObj2 = new ExpressionEvaluator(nextTuple, scanObj.getSchema());
				Expression curr = where.get(i);
				curr.accept(exprObj2);
				Boolean result = Boolean.parseBoolean(exprObj2.getValue());
				allTrue = allTrue && result;
			}

			// Loop through all of the extra union find constraints
			for (String key : ufRestraints.keySet()) {
				// The key value must be here since we check the conditions before adding to our
				// list
				int schema_location = this.getSchema().indexOf(key);
				// Given the schema location here we can then findt the value in the tuple that
				// we are given
				int tuple_value = Integer.parseInt(nextTuple.getTuple().get(schema_location));
				Integer min_value = ufRestraints.get(key).get(0);
				Integer max_value = ufRestraints.get(key).get(1);
				if (tuple_value >= min_value && tuple_value <= max_value) {
					allTrue = allTrue && true;
				} else {
					// Violates the condition
					allTrue = allTrue && false;
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
	 * Retrieves the expressions for this table.
	 * @return ArrayList<Expression> a list of the conditions for the table.
	 */
	public ArrayList<Expression> getWhere(){
		return this.where;
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

	public HashMap<String, ArrayList<Integer>> getUfConstraints() {
		return this.ufRestraints;
	}

	private String toStringHelper(String column, Integer min, Integer max) {
		String ret = "";

		boolean used = false;
		if (min == Integer.MIN_VALUE) {
			;
		} else {
			ret = ret + column + " >= " + min;
			used = true;
		}

		if (max == Integer.MAX_VALUE) {
			;
		} else {
			if (!used) {
				ret = ret + column + " <= " + max;
			} else {
				ret = ret + ", " + column + " <= " + max;
			}
		}

		return ret;
	}

	/**
	 * Gets the string to print for the physical plan
	 * 
	 * @param level the level of the operator
	 * @return the physical plan in string form
	 */
	public String toString(int level) {
//		System.out.println("callled for select operator");
		// where expression
		String wherePortion = "";
		for(int i=0;i<where.size();i++) {
			if(i==where.size()-1) {
				wherePortion=wherePortion+ where.get(i).toString();
			}
			else {
				wherePortion=wherePortion+", "+where.get(i).toString();
			}
		}

		String unionFindPortion = "";
//		System.out.println("++++++++++++++++++++++++");
//		System.out.println(this.ufRestraints);
//		System.out.println("===========================");
		// Add union constraints.
		HashMap<String, ArrayList<Integer>> ufConstraints = this.getUfConstraints();
		boolean used = false;
		for (String key : ufConstraints.keySet()) {
			if (this.toStringHelper(key, ufConstraints.get(key).get(0), ufConstraints.get(key).get(1)).length() > 0) {
				if (!(used)) {
					unionFindPortion = unionFindPortion
							+ this.toStringHelper(key, ufConstraints.get(key).get(0), ufConstraints.get(key).get(1));
					used = true;
				} else {
					unionFindPortion = unionFindPortion + ", "
							+ this.toStringHelper(key, ufConstraints.get(key).get(0), ufConstraints.get(key).get(1));
				}
			}
		}

		String combinedWhere = "";
		if (wherePortion.length() > 0 && unionFindPortion.length() > 0) {
			combinedWhere = wherePortion + "," + unionFindPortion;
		} else if (wherePortion.length() > 0 && !(unionFindPortion.length() > 0)) {
			combinedWhere = wherePortion;
		} else if (!(wherePortion.length() > 0) && unionFindPortion.length() > 0) {
			combinedWhere = unionFindPortion;
		}
//		System.out.println("what is this: " + combinedWhere);

		return "-".repeat(level) + "Select[" + combinedWhere + "]\n" + scanObj.toString(level + 1);
	}

}