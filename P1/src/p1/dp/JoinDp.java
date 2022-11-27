package p1.dp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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
 * A class that computes the order with minimum cost to join using dynamic programming.
 * Should only call this method for more than two relation joins.
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
		
		//initializing every possible pair cost
		for(int i = 0; i < numChil;i++) {
			LogicalOperator left = lop.get(i); 
			LogicalOperator right = lop.get((i+1) % numChil); //this gets every single possible pair
			float leftV; String leftTableName="";float rightV; String rightTableName=""; //initialize left right V and table name
			
			
			HashMap<String, Float> leftInfo = vValue(left);
			for(String key:leftInfo.keySet()) {
				leftV = leftInfo.get(key);
				vValues.put(key, leftV); //put in vValue hashmap
			}
			

			HashMap<String, Float> rightInfo = vValue(right);
			for(String key:rightInfo.keySet()) {
				rightV = rightInfo.get(key);
				vValues.put(key, rightV); //put in vValue hashmap
			}
			
			leftTableName = tableName(left);
			rightTableName = tableName(right);
			float totalV=0;
			if (checkEqualityContained(leftTableName, rightTableName)) {
				//loop through and find all the expressions involving the two table names
				float denominator = findDenominator(leftTableName,rightTableName);
				totalV = (float) (dbStatsInfo.get(leftTableName)[0] * dbStatsInfo.get(rightTableName)[0]) / denominator; 
			} else {
				totalV = (dbStatsInfo.get(leftTableName)[0] * dbStatsInfo.get(rightTableName)[0]);
			}
			
			ArrayList<String> finalName = new ArrayList<String>();
			finalName.add(leftTableName);
			finalName.add(rightTableName);
			String[] casted = (String[]) finalName.toArray();
			
			memoization.put(casted, totalV);
			
		}
//		System.out.println(vValues);
//		System.out.println("memoization is here: ");
//		System.out.println(memoization);
		System.out.println("my dude");
		for(String[] k : dp().keySet()) {
			System.out.println(k);
		}
	}
	
	/**
	 * Function is used to calculate the denominator of the intermediate join cost.
	 * Depending on whether there is an equality in the join condition or not
	 * the equation is different and this function helps differentiate that.
	 * @param ltn left child table name
	 * @param rtn right child table name
	 * @return true if the join condition between two tables have an equality condition
	 * false otherwise.
	 */
	private Boolean checkEqualityContained(String ltn, String rtn) {
		Boolean res = false;
		
		for(String[] key : allConditions.keySet()) {
			if(containTable(key, ltn) && containTable(key,rtn)) {
				for (Expression exp : allConditions.get(key)) {
					if(exp instanceof EqualsTo) {
						res = true;
						break;
					}
				}
			}
		}
		return res;
	}
	//when we have equality use the formula to calculate intermediate size
	/**
	 * Function that calculates the denominator of the intermediate join cost equation.
	 * This function is used when the join condition has an equality condition
	 * @param ltn left child's table name
	 * @param rtn right child's table name
	 * @return the denominator of the intermediate join cost equation
	 */
	private float findDenominator(String ltn, String rtn) {
		float finalV = 1;
		//get the relevant join conditions for current two tables
		for(String[] key : allConditions.keySet()) {
			if(containTable(key, ltn) && containTable(key,rtn)) {
				for (Expression exp : allConditions.get(key)) {
					//We only consider equality condition, if join condition does not contain equality then cross product
					if(exp instanceof EqualsTo) { //we are only considering equality
						EqualsTo expCpy = (EqualsTo) exp;
//						System.out.println("What will this print?");
						
						//get left V value
						String leftExp = expCpy.getLeftExpression().toString();
						String[] parsedl = leftExp.split("\\.");
						float leftV=1;
						
						if(vValues.get(Aliases.getTable(parsedl[0]+"."+parsedl[parsedl.length-1])) != null) {
							leftV = vValues.get(Aliases.getTable(parsedl[0]+"."+parsedl[parsedl.length-1]));
						} else {
							leftV = vValues.get(Aliases.getTable(parsedl[0]));
						}
						
						//get right V value 
						String rightExp = expCpy.getRightExpression().toString();
						String[] parsedr = rightExp.split("\\.");
						float rightV =1;
						if(vValues.get(Aliases.getTable(parsedr[0]+"."+parsedr[parsedl.length-1])) != null) {
							leftV = vValues.get(Aliases.getTable(parsedr[0]+"."+parsedr[parsedl.length-1]));
						} else {
							leftV = vValues.get(Aliases.getTable(parsedr[0]));
						}
						//find the max and update finalV
						finalV  = finalV * Math.max(leftV, rightV);
					}
				}
			}
		}
		return finalV;
	}
	
	/**
	 * Makes the table name from the alias if it is aliased
	 * @param op is the logical operator
	 * @return the real table name specified in data
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
	 * calculates the vValue specified in the hand out (case 1 and case 2).
	 * The table name does not have a .columnName, this is because the v-value
	 * will be the same for all table's columns.
	 * @param op logical operator
	 * @return table name, v value pair
	 */
	private HashMap<String, Float> vValue(LogicalOperator op){
		HashMap<String, Float> nameValue = new HashMap<String, Float>();
		
		float value=0; //key value pair to store in memoization (value is the V value)
		String name="";
		if (op instanceof LogicalScan) {
			//calculate v value (this is case 1)
			LogicalScan cpy = (LogicalScan) op;
			String tableCName = "";
			for(String[] keys : allConditions.keySet()) { //looping through conditions to find column
				if (containTable(keys,cpy.getFromTable())){ //get the column
					tableCName = getTableColumnName(keys, cpy,tableCName);
					name = tableCName;
//					System.out.println("my name is: " +name);
					 
				}
			
			for(int i=0;i<2;i++) {
				if(i==0) {
					value -= dbStatsInfo.get(tableCName)[i];
				} else {
					value += dbStatsInfo.get(tableCName)[i];
				}
			}
			value += 1;
			}
			
		} else {
			//this is case 2
			LogicalFilter cpy = (LogicalFilter) op;
			LogicalScan cpy2 = (LogicalScan) cpy.getChild();
			float scaleFactor = 1;
			name = Aliases.getTable(cpy2.getFromTable());
			for(UnionFindElement constraint : cpy.getUfRestraints()) {
				for (String c : constraint.getAttributeSet()) {
					String[] tableN = c.split("\\.");
					if(Aliases.getTable(tableN[0])   == Aliases.getTable( cpy2.getFromTable())) {
						String[] split = c.split("\\.");
						String tempName = Aliases.getTable(cpy2.getFromTable()) + "."+split[split.length-1];
						int[] minMax = dbStatsInfo.get(tempName);
						
//						System.out.println(c);
//						System.out.println(cpy2.getFromTable());
						
						int range = minMax[1] - minMax[0]; //range of the column
						
						//find scale to update scaleFactor
						float scale = 1;
						if ((float)(constraint.getMaxValue()-constraint.getMinValue()) == (float) Integer.MAX_VALUE - Integer.MIN_VALUE) {
							 scale = 1;
							 
							 //simply don't scale in this case
						} else if (constraint.getMaxValue() == Integer.MAX_VALUE && constraint.getMinValue() != Integer.MIN_VALUE) {
							 scale = (range - (range - constraint.getMinValue()))/range;
							 
						} else if (constraint.getMaxValue() != Integer.MAX_VALUE && constraint.getMinValue() == Integer.MIN_VALUE) {
							 scale = (range - (range - constraint.getMaxValue()))/range;
							
						} else {
							 scale = (float)(constraint.getMaxValue()-constraint.getMinValue()) / range;
						}
						scaleFactor = (float) scaleFactor * scale;
					}
				}
			}
			value = (float) dbStatsInfo.get(Aliases.getTable(cpy2.getFromTable()))[0] * scaleFactor;
//			System.out.println("this table: " + name +", has value: " +value);
			if (value < 1) {
				value = 1;
			}
		}
//		System.out.println("nameeeEe: " +name);
		nameValue.put(name, value);
		return nameValue;
	}

	/**
	 * gets the final table.column name to look up min/max in stats.txt
	 * @param keys is the keys in conditions that contains the current table name
	 * @param cpy is the Logical Scan Operator
	 * @param tableCName is the return value
	 * @return string table.column 
	 */
	private String getTableColumnName(String[] keys, LogicalScan cpy, String tableCName) {
		for (String s: allConditions.get(keys).toString().split(" ")) {
			if(s.contains(cpy.getFromTable())) {
				char[] ca = s.toCharArray();
				for(int iii=0;iii<ca.length-1;iii++) {
					String ss = ".";
					if (ca[iii] == ss.charAt(0)) {
						tableCName = Aliases.getTable(cpy.getFromTable())+"."+ca[iii+1];
					}
				}
			}
		}
		return tableCName;
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
	
	
	private String findUnusedTable(ArrayList<String> keys, ArrayList<String> original){
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
		HashMap<String[],Float> minSet = new HashMap<String[],Float>(); //return variable
		ArrayList<String> minKeys = new ArrayList<String>(); //keys with min cost
		float minVal=0; //min cost
		for(int window=3; window <= numChil; window++) {
			ArrayList<String> temp = tableNames;
			if(window == numChil) { //now choose window-1 key with smallest cost (this is the min cost join order)
				for(String[] key : memoization.keySet()) {
					if(key.size() == window - 1) {
						if(minSet.isEmpty()) {
							minKeys = key;
							minKeys.add(findUnusedTable(key,temp));
							System.out.println(key);
							System.out.println(memoization.get(key));
							minVal = memoization.get(key);
						} else if(minVal > (memoization.get(key))){
							minKeys = key;
							minVal = memoization.get(key);
						}
					}
				}
			} else {
				for(ArrayList<String> key : memoization.keySet()) {
					for(String table : key) {
						temp.remove(table);
					} //this way temp is left with just one more table to join
					//now within one key, try to add the last one
				}
			}
		}
		

		minSet.put(minKeys, minVal);
		return minSet;
	}
}




////using initialized bottom to calculate order of join with min cost
//for(int window=3; window<numTable-1; window++) { 
//	//window represents intermediate join size (disregard final join cost, so -1)
//	List<String> temp = allTables;
//	for(ArrayList<String>key:memoization.keySet()) {
//		if (key.size()==window-1) {
//			for(String table : key) { //loop through the key list
//				temp.remove(table); //remove the tables joined from temp array
//			}
//			//calculate cost for joining one more table
//			for(String tableInstance : temp) { //change this
//				//use the formula here to calculate join cost and add to memoization
//				
//				
//				
//				
//			}
//		}
//	}
//	
//	
//}






	