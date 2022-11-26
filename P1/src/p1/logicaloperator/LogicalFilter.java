package p1.logicaloperator;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import p1.unionfind.UnionFindElement;

/**
 * Logical version of SelectOperator.
 */
public class LogicalFilter extends LogicalOperator {

	// The child operator
	private LogicalOperator child;
	// The expression to filter by
	private ArrayList<Expression> exp;
	// The constraints from the unionfind
	private ArrayList<UnionFindElement> ufRestraints;
	// The unionFind contraints relevant for this table
	private HashMap<String,ArrayList<Integer>> attrConstraints;

	/**
	 * The constructor for the logical filter operator
	 *
	 * @param op The child operator
	 * @param ex Expression containing the information
	 */
	public LogicalFilter(LogicalOperator op, ArrayList<Expression> expr, ArrayList<UnionFindElement> ufRestraints) {
		this.child = op;
		this.exp = expr;
		this.ufRestraints=ufRestraints;
	}
	// Changes
	/**
	 * Retrieves the child operator.
	 *
	 * @return The child operator used to get tuples.
	 */
	public LogicalOperator getChild() {
		return child;
	}

	/**
	 * Retrieves the expression containing the conditions that we are filtering on
	 *
	 * @return An Expression object containing the condition information.
	 */
	public ArrayList<Expression> getExpression() {
		return exp;
	}

	// This is just for testing and knowing that we have the right node placement.
	public String toString() {
		return "This is a logical filter node";
	}
	
	/**
	 * Get the constraints from the unionfind
	 * @return ArrayList<UnionFindElement> containing the restraint information
	 */
	public ArrayList<UnionFindElement> getUfRestraints(){
		return this.ufRestraints;
	}
	
	/**
	 * Sets the relevant constraints for the table
	 * @param constraints Attribute constraints for our table.
	 */
	public void setRelevantConstraints(HashMap<String,ArrayList<Integer>> constraints) {
		this.attrConstraints=constraints;
	}
	
	public HashMap<String,ArrayList<Integer>> getRelevantConstraints(){
		return this.attrConstraints;
	}
	
	/**
	 * Helper method for toString
	 * @return String for the unionfind constraints if any.
	 */
	private String toStringHelper(String column, Integer min, Integer max) {
		String ret=""; // Return string. Null if there are no matches or the hashmap is empty;
		
		boolean used=false;
		if(min==Integer.MIN_VALUE) {
			;
		}
		else {
			ret=ret+column+">="+min;
			used=true;
		}
		
		if(max==Integer.MAX_VALUE) {
			;
		}
		else {
			if(!used) {
				ret=ret+column+"<="+max;
			}
			else {
				ret=ret+","+column+"<="+max;
			}
		}
		
		return ret;
	}

	/**
	 * Gets the string to print for the logical plan
	 * 
	 * @param level the level of the operator
	 * @return the logical plan in string form
	 */
	public String toString(int level) {
		String wherePortion="";
		for(int i=0;i<exp.size();i++) {
			if(i==0) {
				wherePortion=wherePortion+ exp.get(i).toString();
			}
			else {
				wherePortion=wherePortion+", "+ exp.get(i).toString();
			}
		}
		
		System.out.println("Where portion of the logical filter reached");
		System.out.println(wherePortion);
		System.out.println("++++++++++ something is delimited here");
		
		String unionFindPortion="";
		HashMap<String,ArrayList<Integer>> ufConstraints = this.getRelevantConstraints();
		boolean used=false;
		for(String key: ufConstraints.keySet()) {
			if(this.toStringHelper(key, ufConstraints.get(key).get(0), ufConstraints.get(key).get(1)).length()>0) {
				if(!used) {
					unionFindPortion=unionFindPortion+this.toStringHelper(key, ufConstraints.get(key).get(0), ufConstraints.get(key).get(1));
					used=true;
				}
				else {
					unionFindPortion=unionFindPortion+", "+this.toStringHelper(key, ufConstraints.get(key).get(0), ufConstraints.get(key).get(1));
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
		
//		System.out.println("Entered inside of this loop");
//		System.out.println(combinedWhere);
		
		return "-".repeat(level) + "Select[" + combinedWhere + "]\n" + child.toString(level + 1);
	}
}
