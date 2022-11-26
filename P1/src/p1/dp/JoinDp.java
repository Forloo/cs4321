package p1.dp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import p1.logicaloperator.LogicalAllJoin;
import p1.util.Aliases;

/**
 * A class that computes the order with minimum cost to join using dynamic programming.
 * Should only call this method for more than two relation joins.
 * @author Jinseok Oh
 *
 */
public class JoinDp {

	
	
	//TODO: compute V value of each join, find smallest cost logical plan
	
	//To calculate the join order with lowest cost
	
	//cost: cost of plan + intermediate join size
	
	//cost of plan: sum of size in tuple of all intermediate relation
	//	Don't include the last table join cost (A,B,C then A join B is the cost of all)
	
	//computing intermediate relation size
	//	given relation, Cardinality of table on numerator multiplied 
	//		over max of attribute per select statement in denominator multiplied
	
	//if just non-equality then cross product
	//if mix then just consider equality
	
	
	//Cost of plan: sum of intermediate results
	//	sum of intermediate results: calculated using the formula specified
	//	V values: calculated using the formulas
	
	//TODO: calculate V values and set up DP
	
	
	
	//all tables of the join all logical operator
	private List<String> allTables;
	//all conditions of the join all operator
	private HashMap<String[], ArrayList<Expression>> allConditions;
	//min cost of join order
	private int minCost;
	//variable with stats text file information
	private HashMap<String, int[]> dbStatsInfo;
	
	//constructor
	public JoinDp(LogicalAllJoin logicalJoin, HashMap<String, int[]> dbStatsInfo){
		//calulating V values (round to 1 if less than 1, can't be zero)
		
		//case 1: relation is a base table R (simply max - min + 1)
		
		//case 2: relation is selection based on table R (start with case 1
		//	and reduce) disregard non-obvious ones
		
//		System.out.println("INSIDE" + dbStatsInfo);
		
		
		//case 3: relation of two joins
//		System.out.println("insideee");
		this.allTables = logicalJoin.getTableNames();
		System.out.println(logicalJoin.getTableNames());
		this.allConditions = logicalJoin.getConditions();
		
		
		//I GOT EVERYTHING, JUST NEED TO CALCULATE THE COST AND DO DP AND COMPARE DP'd ANSWER
		this.dbStatsInfo = dbStatsInfo;
		
		
		for(String[] keys : allConditions.keySet()) {
			System.out.println("========================");
			for (int l = 0; l<keys.length;l++) {
				System.out.println(keys[l]);
				System.out.println("Real Name: " + Aliases.getTable(keys[l])); 
			}
		}
		
		
		
		int numRelations = 0;
		//brute force -> dp -> back trace -> return trace
		
//		minCost(allTables,allTables.size(),initializeMin(allTables));
		
		
		int min = 0;
		
		
		//memoization, array list of table names as keys
		// array list of whatever things I need as value
		//		I can make a custom data type with fields in it 
		//		to store costs and etc.
		
		
		//try this
		//might have to implement key compare 
		//	where you compare the complement of the keys
		
		
		
		
		
		
		
		
		
		dp();//change this
		this.minCost = min;
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
		
		//dynamic programming solution (bottom up)
		//initializing bottom (every possible pair cost stored in memoization)
		for(int window = 2; window < numTable-1; window++) {
			for(int i = 0; i < numTable;i++) {
				//get tables, use the formula to calculate
				
				
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
