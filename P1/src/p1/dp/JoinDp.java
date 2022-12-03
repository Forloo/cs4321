package p1.dp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import p1.logicaloperator.LogicalAllJoin;
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalOperator;
import p1.logicaloperator.LogicalScan;
import p1.operator.Operator;
import p1.unionfind.UnionFindElement;
import p1.util.Aliases;
import p1.util.ExpressionParser;
import p1.util.LogicalPlan;
import p1.util.QueryPlan;

/**
 * A class that computes the order with minimum cost join using dynamic programming.
 * One variable for memoization, one variable for original v values. v-values get updated
 * throughout the dp algorithm whenever there is a calculation needed for v values from 
 * a join of two other relations.
 * @author Jinseok Oh
 *
 */
public class JoinDp {
	//all conditions of the join all operator
	private HashMap<String[], ArrayList<Expression>> allConditions;
	//min cost of join order
	private int minCost;
	//variable with stats text file information
	private HashMap<String, int[]> dbStatsInfo;
	//stores the logical all join's children
	private List<LogicalOperator> lop;
	//stores v values
	private HashMap<String, Float> vValues;
	private HashMap<String, Float> vValueN;
	//store number of logical all join children
	private int numChil;
	//table names that are being joined
	private ArrayList<String> tableNames = new ArrayList<String>();
	//memoized array (key size is always window - 1)
	private HashMap<String[],Float> memoization;
	
	//constructor
	public JoinDp(LogicalAllJoin logicalJoin, HashMap<String, int[]> dbStatsInfo){
		//initializing variables
		for(String s: logicalJoin.getTableNames()) {
			tableNames.add(s.split(" ")[0]);
		}
		this.lop = logicalJoin.getTableOperators();
		this.allConditions = logicalJoin.getConditions();
		this.dbStatsInfo = dbStatsInfo;
		numChil= lop.size();
		memoization = new HashMap<String[],Float>();
		//stores the vValues involved in the relation
		vValues = new HashMap<String, Float>();
		vValueN = new HashMap<String, Float>(); //N for new
		System.out.println("constructor entered");
		//initializing all v values
		initV();
		//initializing every possible pair cost for base case, then we build off
		initPairs();
	}
	
	/**
	 * Returns the order of the function. Value is zero when there are less than
	 * three relations.
	 * @return key value pair where the key represents the order and value the cost.
	 */
	public HashMap<String[], Float> getOrder(){
		return dp();
	}
	
	/**
	 * Function that calculates the denominator of the intermediate join cost equation.
	 * This function is used when the join condition has an equality condition, so it 
	 * disregards the non equality conditions.
	 * @param ltn left child's table name
	 * @param rtn right child's table name
	 * @return the denominator of the intermediate join cost equation
	 */
	private float findDenominator(String[] ltn, String rtn) {
		float finalV = 1;
		//get the relevant join conditions for current two tables
		for(String[] key : allConditions.keySet()) { //per one condition, calcalate max and multiply
			if(containTable(key, ltn[0]) && containTable(key,rtn)) { //while looping through conditions, we found a match
				for (Expression exp : allConditions.get(key)) {
					//We only consider equality condition, if join condition does not contain equality then cross product
					//this handled outside of this function
					if(exp instanceof EqualsTo) {
						EqualsTo expCpy = (EqualsTo) exp;
						//get left V value (THIS IS THE THIRD CASE OF INTERMEDIATE JOIN SIZE)
						//ED-DISCUSSION EXAMPLE IS SIMPLY MAX VALUES, BUT ACCORDING TO THE INSTRUCTIONS, IT IS MIN VALUE SO 
						//	WE SIMPLY GET THE MIN VALUE OF THE ORIGINAL AND CALCULATE.
						float leftV = -1;
						for(String leftT : ltn) { //simply get the min value of attributes involved from original v-values (like on ed-discussion answer's example)
							String leftExp = expCpy.getLeftExpression().toString();
							String[] parsedl = leftExp.split("\\.");
							if(leftV == -1) {
								leftV = vValueN.get(Aliases.getTable(parsedl[0])+"."+parsedl[parsedl.length-1]);
							} else if(leftV > vValueN.get(Aliases.getTable(parsedl[0]))) {
								leftV =  vValueN.get(Aliases.getTable(parsedl[0]));
							}
						}
						//get right V value (simply the column it gets equated to)
						String rightExp = expCpy.getRightExpression().toString();
						String[] parsedr = rightExp.split("\\.");
						float rightV =vValueN.get(Aliases.getTable(parsedr[0])+"."+parsedr[parsedr.length - 1]);
						finalV  = finalV * Math.max(leftV, rightV);
					} else { //disregard other than equality constraints
					}
				}
			}
		}
		return finalV;
	}
	
	/**
	 * Function is used to calculate the denominator of the intermediate join cost.
	 * Depending on whether there is an equality in the join condition or not
	 * the equation is different and this function helps differentiate that.
	 * @param ltn left child table name (array since it could be a product of join
	 * @param rtn right child table name
	 * @return true if the join condition between two tables have an equality condition
	 * false otherwise.
	 */
	private Boolean checkEqualityContained(String[] ltn, String rtn) {
		Boolean res = false;
		
		for(String[] key : allConditions.keySet()) {
			for(String ltb : ltn) {
				if(containTable(key, ltb) && containTable(key,rtn)) {
					for (Expression exp : allConditions.get(key)) {
						if(exp instanceof EqualsTo) {
							res = true;
							break;
						}
					}
				}
			}
			
		}
		return res;
	}
	
	
	/**
	 * Initializes the memoization dictionary for dynamic programming 
	 * with every possible pairs of relations and their join cost.
	 */
	private void initPairs() { 
		System.out.println("-initializing pairs: " + numChil);
		for(int i = 0; i < numChil;i++) { //match ith operator with the rest
			
			//set the left table
			LogicalOperator left = lop.get(i);
			String[] leftTableName= new String[1]; leftTableName[0] = tableName(left);
			int j = (i+1) % numChil;
			while(j != i) {
				
				//set the right table
				LogicalOperator right = lop.get(j);
				String rightTableName=""; //initialize left right V and table name
				rightTableName = tableName(right);
				String[] finalName = new String[2];
				finalName[0] = leftTableName[0];
				finalName[1] = rightTableName;
				Arrays.sort(finalName); //sort to avoid counting A,B and B,A separately (fine six max array size is 2)
				
				Boolean exist = false; //false when key doesn't exist in it
				for(String[] keyInMem : memoization.keySet()) {
					if (keyInMem[0] == finalName[0] && keyInMem[1] == finalName[1]) {
						exist = true;
						break;
					}
				}
				
				if (!exist) { //only when not in memoization, calculate and store
					float totalV=0;
					if (checkEqualityContained(leftTableName, rightTableName)) {
						System.out.println("--"+leftTableName[0] + " and " + rightTableName + " have equality");
						String[] ltn = new String[1]; //left table is just 1 for base row of dictionary
						ltn[0] = leftTableName[0];
						float denominator = findDenominator(ltn,rightTableName);
						System.out.println("--denominator calculated for this is " + denominator);
						totalV = (float) (dbStatsInfo.get(leftTableName[0])[0] * dbStatsInfo.get(rightTableName)[0]) / denominator; 
					} else {
						System.out.println("--no equality so cross product: " + (dbStatsInfo.get(leftTableName[0])[0] + " multiply " + dbStatsInfo.get(rightTableName)[0]));
						totalV = (dbStatsInfo.get(leftTableName[0])[0] * dbStatsInfo.get(rightTableName)[0]);
					}
					
					System.out.println("\n---adding this pair: " + finalName[0] + " and " + finalName[1] + " to memoized hashmap\n");
					memoization.put(finalName, totalV);
				}
				
				j = (j+1) % numChil; //change j to count for every possible pair
			}
			
			
		}
		System.out.println("=============================================");
		System.out.println("final memoized hashmap is this: " + memoization.size());
	}
	
	
	/**
	 * Initializes the v-values for intermediate join cost calculation.
	 * 
	 * At first, the function assumes every relation is a base table R and 
	 * store (Relation.column, v-value) pair into vValueN. v-value here is calculated 
	 * using column.max-column.min+1.
	 * 
	 * Then the function takes care of selection on a base table relation. For a 
	 * child that is a selection, the function loops through the selection conditions,
	 * sets all attributes of same relation's v-values to be the minimum among all. 
	 * v-value = scale factor * number of tuples in relation assuming uniform distribution.
	 */
	private void initV() {
		System.out.println("-initializing V");
		
		//loop through db stats and initialize all relations' attributes as if case 1
		for(String key : dbStatsInfo.keySet()) {
			if(key.contains(".")) { //this means it is an attribute
				int nums = dbStatsInfo.get(key)[1] - dbStatsInfo.get(key)[0] + 1;
				vValueN.put(key, (float) nums); //store for case 1 of v-values
			}
		}
		System.out.print("--this is the v values calculated using max - min + 1: \n");
		System.out.println(vValueN);
		//looping through each child of logical all join (take care of case 2)
		for(int i = 0; i < numChil; i++) {
			LogicalOperator childOfLop = lop.get(i); //get one of the child of LogicalAllJoin
			if(childOfLop instanceof LogicalFilter) { //then case 2 and must update all attribute v-values of this relation
				System.out.println("--this op is a selection so re-write v-values");
				LogicalFilter cpy = (LogicalFilter) childOfLop;
				LogicalScan cpy2 = (LogicalScan) cpy.getChild();
				
				//find local min v-value among attributes in same relation by looping through the select constraints
				float localMinV = -1;
				for(UnionFindElement constraint : cpy.getUfRestraints()) {
					//each iteration, you find the scale factor and compute individual v-value of attribute of same relation
					for (String c : constraint.getAttributeSet()) { //attribute set
						String[] tableN = c.split("\\.");
						if(Aliases.getTable(tableN[0]) == Aliases.getTable(cpy2.getFromTable())) { //if current relation's attribute involved in select
							String tempName = Aliases.getTable(cpy2.getFromTable()) + "."+tableN[tableN.length-1]; //table with column
							System.out.println("---calculating V value for this: " + tempName);
							int[] minMax = dbStatsInfo.get(tempName); //get min max of this attribute
							int range = minMax[1] - minMax[0]; //range of the column
							float scale = 1;
							System.out.println("Column max: " +  minMax[1] + " Column min: " + minMax[0]);
							System.out.println("constraints are: " + constraint.getMaxValue() + " " + constraint.getMinValue());
							if (constraint.getMaxValue() == Integer.MAX_VALUE  && constraint.getMinValue() == Integer.MIN_VALUE) {
								System.out.println("----no constraints! scale is: 1");
								 scale = 1;
								 //simply don't scale in this case
							} else if (constraint.getMaxValue() == Integer.MAX_VALUE && constraint.getMinValue() != Integer.MIN_VALUE) {
								 scale = (minMax[1] - constraint.getMinValue())/range;
								 System.out.println("----only lower bound! scale is: " + scale); 
							} else if (constraint.getMaxValue() != Integer.MAX_VALUE && constraint.getMinValue() == Integer.MIN_VALUE) {
								scale = (constraint.getMaxValue() - minMax[0])/range;
								System.out.println("----only upper bound! scale is: " + scale);
							} else {
								 scale = (float)(constraint.getMaxValue()-constraint.getMinValue()) / range;
								 System.out.println("----bounded up and low! scale is:  " + scale);
							}
							float nn = dbStatsInfo.get(Aliases.getTable(cpy2.getFromTable()))[0] * scale; //calculate individual attribute v-val and
							System.out.println("temp v-value: " +nn);
							if (localMinV == -1 || nn < localMinV) { //update local Min V
								localMinV = nn;
							}
						}
					}
				}
				
				if (localMinV < 1) { //counting when too small
					localMinV = 1;
				}
				
				//now loop through vValueN and update the min values
				for (String key : vValueN.keySet()) {
					if(key.contains(Aliases.getTable(cpy2.getFromTable()))) { //name of current table
						System.out.println("----overwriting " + key + " with " + localMinV);
						vValueN.put(key, localMinV); //overwrite to local min v-values
					}
				}
			}
		}
		System.out.println("final vValueN dictionary: " + vValueN);
	}
			
	/**
	 * Returns the full table name regardless of aliased or not and 
	 * regardless of logical operator type. Logical operator is either
	 * logical scan or logical filter, since children of logical 
	 * all join is either one of those two.
	 *  
	 * @param op is the logical operator (child of logical all join)
	 * @return the real table name of op
	 */
	private String tableName(LogicalOperator op) {
		if (op instanceof LogicalScan) {
			LogicalScan cpy = (LogicalScan) op;
			return Aliases.getTable(cpy.getFromTable());
		} else {
			LogicalFilter cpy = (LogicalFilter) op;
			LogicalScan cpy2 = (LogicalScan) cpy.getChild();
			return Aliases.getTable(cpy2.getFromTable());
		}
	}
	

	/**
	 * checks if the key list in stats.txt file contain the table name
	 * This function is used to get the column used for one table in join
	 * @param list of keys in stats.txt
	 * @param tableName of table we are looking for
	 * @return true if table name is included in the keys list. false otherwise.
	 */
	private Boolean containTable(String[] list, String tableName) {
		for(String  table : list) {
			if (Aliases.getTable(table) == Aliases.getTable(tableName)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Finds the last unused table after calculating all the intermediate join costs.
	 * The last table is appended to the list of tables to output the ordering
	 * of join that produces minimum cost from the dp function.
	 * @param keys are keys in memoization dictionary
	 * @param original are the list of tables involved in logical all join
	 * @return name of the unused relation
	 */
	private String findUnusedTable(String[] keys, ArrayList<String> original){
		ArrayList<String> unused = new ArrayList<String>();
		for(String k : keys) {
			original.remove(k);
		}
		return original.get(0);
	}
	
	/**
	 * function that calculates the min cost join order using dynamic programming. 
	 * Min order is the order of key from left to right.
	 * @return key value pair where key contains the order to join and value, the min cost of join
	 */
	private HashMap<String[],Float> dp() {
		if (numChil < 3 ) {
			HashMap<String[],Float> res = new HashMap<String[], Float>();
			String[] key = new String[2];
			for (int i=0; i<2;i++) {
				key[i] = tableNames.get(i);
			}
			res.put(key, null);
			return res;
		}
		HashMap<String[],Float> minSet = new HashMap<String[],Float>(); //return variable
		String[] minKeys = new String[numChil]; //keys with min cost
		float minVal=0; //min cost
		for(int window=3; window <= numChil; window++) {
			if(window == numChil) { //now choose window-1 key with smallest cost (this is the min cost join order)
				for(String[] key : memoization.keySet()) {
					ArrayList<String> tempIn = (ArrayList<String>) tableNames.clone();
					if(key.length == window - 1) {
						if(minSet.isEmpty()) {
							for (int i =0; i<=window-2;i++) {
								minKeys[i]=key[i];
							}
							minKeys[window-1] = findUnusedTable(key,tempIn);
							minVal = memoization.get(key);
						} else if(minVal > (memoization.get(key))){
							minKeys = key;
							minVal = memoization.get(key);
						}
					}
				}
			} else {
				//intermediate join, must calculate join v values of left child here
				//get unused table list, try adding one by one, calculate intermediate join cost
				HashMap<String[],Float> newMemoized = new HashMap<String[],Float>();
				for(String[] key : memoization.keySet()) {
					if(key.length == window -1) { //only look at window-1 sized keys, rest are irrelevant
						ArrayList<String> temp = (ArrayList<String>) tableNames.clone();
						
						for(String table : key) {
							temp.remove(table);
						}
						//now temp is left with just unused tables
						//traverse temp and add one table by one to current key, calculate intermediate join cost
						for(String table : temp) {
							float newVval=1;
							//for each table, calculate new intermediate join cost
							if(checkEqualityContained(key,table)) {
								for(String tableIn : key) { //this is the numerator
									newVval =  newVval * dbStatsInfo.get(Aliases.getTable(tableIn))[0];
								}
								newVval = newVval / findDenominator(key,table);
							} else { //simply the cross product
								for(String tableIn : key) { 
									newVval =  newVval * dbStatsInfo.get(Aliases.getTable(tableIn))[0];
								}
							}
							//append table to key
							String[] newKey = new String[window];
							for(int i=0;i<=window-2;i++) {
								newKey[i]=key[i];
							}
							newKey[window-1] = table;
							//add the appended key and calculated intermediate join cost value to memoized
							newMemoized.put(newKey, newVval);
						}
					}
				}
				
				//put to memoization here to avoid ConcurrentModificationException
				for( String[] key : newMemoized.keySet()) {
					memoization.put(key, newMemoized.get(key));
				}
			}
			
		}
		minSet.put(minKeys, minVal);
		return minSet;
	}
}