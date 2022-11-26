package p1.logicaloperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Expression;
import p1.unionfind.UnionFind;

public class LogicalAllJoin extends LogicalOperator {

	// Original Join operator implementation
	// 1. Processed all the tables in pairs and then use the previous join as the
	// left child.
	// 2. In each of the joins we have the child being made and then either adding
	// that
	// to an existing join or making the new join

	// New Implementation:
	// 1. Need to use this information later in PhysicalPlanBuilder so we need all
	// of the operators
	// 2. Given the first node we make in the PPBuilder I guess we can store that
	// temporarily
	// and then from that we can get the TableString from that?

	// Expressioninfo is used to get check if the table needs conditions
	// Then we useExpressionInfoAliases to get if the joined table needs any
	// conditons on it.

	// The tablenames for the query: Ordering is the order of the query
	private List<String> tableNames;
	// The operators for each of the tables in the order of the tablenames
	private List<LogicalOperator> tableOperators;
	// The conditions for all the joined tables.
	private HashMap<String[], ArrayList<Expression>> conditions;
	// The union find elements
	private ArrayList<Expression> allExpr;
	// The unionfind object
	private UnionFind uf;

	/**
	 * The constructor for LogicalAllJoin
	 * 
	 * @param tableNames     : The list of string names for the query: Ordering is
	 *                       the order of the query
	 * @param tableOperators : The operator for each of the tables in the same order
	 *                       as tableNames.
	 * @param conditions     : The conditions for all the tables.
	 */
	public LogicalAllJoin(List<String> tableNames, List<LogicalOperator> tableOperators,
			HashMap<String[], ArrayList<Expression>> conditions, ArrayList<Expression> allExpr) {

		this.tableNames = tableNames;
		this.tableOperators = tableOperators;
		this.conditions = conditions;
		this.allExpr=allExpr;
		
	}

	/**
	 * Retrieves a list containing all of the tables we are joining on
	 * 
	 * @return List<String> containing the names of all the tables that we are
	 *         joining.
	 */
	public List<String> getTableNames() {
		return this.tableNames;
	}

	/**
	 * Retrieves all of the Operators for the table we are joining on.
	 * 
	 * @return List<LogicalOperators> for all of the tables that we are joining on.
	 */
	public List<LogicalOperator> getTableOperators() {
		return this.tableOperators;
	}

	/**
	 * Retrieves the conditions for specific table combinations
	 * 
	 * @return A hashmap containing the conditions for the tables.
	 */
	public HashMap<String[], ArrayList<Expression>> getConditions() {
		return this.conditions;
	}
	
	/**
	 * Sets the new conditions for the all join operator.
	 * @param conditions
	 */
	public void setConditions(HashMap<String[],ArrayList<Expression>> conditions) {
		this.conditions=conditions;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Gets the string to print for the logical plan
	 * 
	 * @param level the level of the operator
	 * @return the logical plan in string form
	 */
	public String toString(int level) {
		ArrayList<Expression> wheres = new ArrayList<Expression>();
		for (Map.Entry<String[], ArrayList<Expression>> w : conditions.entrySet()) {
			wheres.addAll(w.getValue());
		}
		List<String> whereStr = wheres.stream().map(s -> s.toString()).collect(Collectors.toList());
		String lines = "-".repeat(level) + "Join[" + String.join(" AND ", whereStr) + "]\n";

		// Print union find
		lines += uf.toStringFile();

		for (LogicalOperator op : tableOperators) {
			lines += op.toString(level + 1);
		}
		return lines;
	}

	/**
	 * Sets the items in union find so we can print them in toString(int level)
	 * 
	 * @param findings the union find elements
	 */
	public void setUnionFind(UnionFind findings) {
		uf = findings;
	}
	
	/**
	 * Retrieves the UnionFind
	 * @return UnionFind representing joined conditions and unused expressions.
	 */
	public UnionFind getUnionFind() {
		return this.uf;
	}
	
	public ArrayList<Expression> getUnusedOperators(){
		return this.allExpr;
	}
	
}
