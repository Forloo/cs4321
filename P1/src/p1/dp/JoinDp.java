package p1.dp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import p1.logicaloperator.LogicalAllJoin;
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalOperator;
import p1.logicaloperator.LogicalScan;
import p1.operator.Operator;
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
	
	//constructor
	public JoinDp(LogicalAllJoin logicalJoin, HashMap<String, int[]> dbStatsInfo){
		this.lop = logicalJoin.getTableOperators();
		this.allConditions = logicalJoin.getConditions();
		this.dbStatsInfo = dbStatsInfo;
		int numChildren = lop.size();
		HashMap<ArrayList<String>,Integer> memoization = new HashMap<ArrayList<String>,Integer>();
		//stores the vValues involved in the relation
		HashMap<String, Integer> vValues = new HashMap<String, Integer>();
		
		//initializing every possible pair cost
//		for(int i = 0; i < numChildren;i++) {
//			LogicalOperator left = lop.get(i); 
//			LogicalOperator right = lop.get((i+1) % numChildren); //this gets every single possible pair
//			int leftV; String leftTableName="";int rightV; String rightTableName=""; //initialize left right V and table name
//			
//			
//			HashMap<String, Integer> leftInfo = vValue(left);
//			for(String key:leftInfo.keySet()) {
//				leftV = leftInfo.get(key);
//				vValues.put(key, leftV); //put in vValue hashmap
//			}
//			
//
//			HashMap<String, Integer> rightInfo = vValue(right);
//			for(String key:rightInfo.keySet()) {
//				rightV = rightInfo.get(key);
//				vValues.put(key, rightV); //put in vValue hashmap
//			}
//			
//			leftTableName = tableName(left);
//			rightTableName = tableName(right);
//			int totalV = (dbStatsInfo.get(leftTableName)[0] * dbStatsInfo.get(rightTableName)[0]); 
//			ArrayList<String> finalName=new ArrayList<String>();
//			finalName.add(leftTableName);
//			finalName.add(rightTableName);
//			
//			memoization.put(finalName, totalV);
//		}
	}
	
	
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
	
	
	private HashMap<String, Integer> vValue(LogicalOperator op){
		HashMap<String, Integer> nameValue = new HashMap<String, Integer>();
		int value=0; //key value pair to store in memoization (value is the V value)
		String name="";
		if (op instanceof LogicalScan) {
			//calculate v value (this is case 1)
			LogicalScan cpy = (LogicalScan) op;
			String tableCName = "";
			for(String[] keys : allConditions.keySet()) { //looping through conditions to find column
				if (containTable(keys,cpy.getFromTable())){ //get the column
					tableCName = getTableColumnName(keys, cpy,tableCName);
					name = tableCName;
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
//			cpy.getExpression();
			System.out.println("weird");
			System.out.println(cpy.getExpression()); //just get the expression and parse the string
			String[] parsed = cpy.getExpression().toString().split(" ");
			
			
			
		}
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
	
	
	/**
	 * function that calculates the min cost join order using dynamic programming. 
	 * Min order is the order of key from left to right.
	 * @return key value pair where key contains the order to join and value, the min cost of join
	 */
	private HashMap<ArrayList<String>,Integer> dp() {
		int numTable = allTables.size();
		//table names as key and cost as value
		HashMap<ArrayList<String>,Integer> memoization = new HashMap<ArrayList<String>,Integer>();
		//initializing bottom (every possible pair cost stored in memoization A,B and B,A are the same)
		for(int window = 2; window < numTable-1; window++) {
			for(int i = 0; i < numTable;i++) {
				//get tables, use the formula to calculate
				while(int j = (i+1) % numTable;j<window;j++) {
					
				}
				
			}
		}
		
		
		
		
		//using initialized bottom to calculate order of join with min cost
		for(int window=3; window<numTable-1; window++) { 
			//window represents intermediate join size (disregard final join cost, so -1)
			List<String> temp = allTables;
			for(ArrayList<String>key:memoization.keySet()) {
				if (key.size()==window-1) {
					for(String table : key) { //loop through the key list
						temp.remove(table); //remove the tables joined from temp array
					}
					//calculate cost for joining one more table
					for(String tableInstance : temp) { //change this
						//use the formula here to calculate join cost and add to memoization
						
						
						
						
					}
				}
			}
			
			
		}
		
		return memoization; //change this
	}
	
	
	/**
	 * computes the V value given the tables
	 * @param allTables
	 * @return
	 */
	private int computeV(List<String> allTables){
		return 0;
	}
	
	
}











	