package p1.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.operator.BNLJOperator;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.ExternalSortOperator;
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.SMJOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;
import p1.operator.TNLJOperator;

/**
 * A class that evaluates what operator to use as the root for a query.
 */
public class QueryPlan {

	// The root operator for the queryPlan
	private Operator rootOperator;

	/**
	 * Constructs a query plan object for the given query.
	 *
	 * @param query Inputed query file.
	 * @param db:   Database Catalog object holding table names and their schema.
	 */
	public QueryPlan(Statement query, DatabaseCatalog db) {
		Select select = (Select) query;

		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

		List allColumns = plainSelect.getSelectItems();

		FromItem from = plainSelect.getFromItem();

		Expression where = plainSelect.getWhere();

		List joins = plainSelect.getJoins();

		Distinct distinct = plainSelect.getDistinct();

		List groupByElements = plainSelect.getOrderByElements();

		// Get all of the expression conditions for each of the tables. THIS DOES NOT
		// HANDLE ALIASES.
		// NEED TO DO THE PADDING LATER FOR ALIASES TO WORK.
		HashMap<String[], ArrayList<Expression>> expressionInfoAliases = null;
		HashMap<String, ArrayList<Expression>> expressionInfo = null;
		if (where != null) {
			ExpressionParser parse = new ExpressionParser(where);
			where.accept(parse);
			expressionInfoAliases = parse.getTablesNeeded();
			expressionInfo = parse.getTablesNeededString();
		}

		// Extract aliases
		Aliases.getInstance(plainSelect);
		String fromTable = from.toString();
		if (from.getAlias() != null) {
			fromTable = from.getAlias();
		}

		// This will be the child so that we can pass it into our constructor later.
		Operator child = null;
		boolean joinUsed = false;
		// Ordering
		// Join, select, scan, sort, duplicate elimination operator

		// Check if there is more than one table. If more than one table make join.
		if (joins != null) {
			boolean fromUsed = false;
			Operator prev = null;

			// Iterate through the table and make the join operators
			for (int i = 0; i < joins.size(); i++) {
				String alias = Aliases.getAlias(joins.get(i).toString());
				if (!fromUsed) {
					// If the expressionInfo is null then that means there is no where conditions
					if (expressionInfo == null) {
						ScanOperator first = new ScanOperator(fromTable);
						ScanOperator second = new ScanOperator(alias);
						String combinedName = fromTable + "," + alias;
						Operator temp = createJoinOp(combinedName, first, second, null);
						prev = temp;
					} else {
						// Retrieve the where conditions and then assign them to the right table.
						Operator first = null;
						if (expressionInfo.containsKey(fromTable)) {
							ArrayList<Expression> conditions = expressionInfo.get(fromTable);
							ScanOperator scanone = new ScanOperator(fromTable);
							SelectOperator selectone = new SelectOperator(scanone, conditions.get(0));
							first = selectone;
						} else {
							ScanOperator scanone = new ScanOperator(fromTable);
							first = scanone;
						}

						Operator second = null;
						if (expressionInfo.containsKey(alias)) {
							// Get the arraylist of conditions
							ArrayList<Expression> conditions2 = expressionInfo.get(alias);
							ScanOperator scantwo = new ScanOperator(alias);
							SelectOperator selecttwo = new SelectOperator(scantwo, conditions2.get(0));
							second = selecttwo;
						} else {
							ScanOperator scantwo = new ScanOperator(Aliases.getAlias(joins.get(0).toString()));
							second = scantwo;
						}
						String combinedName = fromTable + "," + alias;
						String[] tablesNeeded = combinedName.split(",");
						Arrays.sort(tablesNeeded);
						String sortedTablesNeeded = String.join(",", tablesNeeded);

						// Check if there is match in the sorted table and the current string.
						// If match then assign the conditions otherwise there are no conditions make it
						// null.
						ArrayList<Expression> joinCondition = null;
						if (expressionInfo.containsKey(sortedTablesNeeded)) {
							ArrayList<Expression> joinConditions = expressionInfo.get(sortedTablesNeeded);
							joinCondition = joinConditions;
						}
						Operator temp = createJoinOp(combinedName, first, second, joinCondition);
						prev = temp;
					}
					fromUsed = true;
				} else // From table is used meaning that there is more than one join operator being
						// used
				// so this means that the left child is a join operator.
				if (expressionInfo == null) {
					ScanOperator first = new ScanOperator(alias);
					String combinedName = prev.getTable() + "," + alias;
					Operator temp = createJoinOp(combinedName, prev, first, null);
					prev = temp;
				} else {
					Operator first = null;
					if (expressionInfo.containsKey(alias)) {
						ArrayList<Expression> conditions = expressionInfo.get(alias);
						ScanOperator scanone = new ScanOperator(alias);
						SelectOperator selectone = new SelectOperator(scanone, conditions.get(0));
						first = selectone;
					} else {
						ScanOperator scanone = new ScanOperator(alias);
						first = scanone;
					}
					String combinedName = prev.getTable() + "," + alias;
					ArrayList<Expression> joinCondition = new ArrayList<Expression>();
					String[] splitted = combinedName.split(",");
					HashSet<String> tblsNeed = new HashSet<String>();
					for (int k = 0; k < splitted.length; k++) {
						tblsNeed.add(splitted[k]);
					}

					for (String[] key : expressionInfoAliases.keySet()) {
						boolean allIncluded = true;
						for (int l = 0; l < key.length; l++) {
							allIncluded = allIncluded && tblsNeed.contains(key[l]);
						}

						if (allIncluded) {
							ArrayList<Expression> allExpr = expressionInfoAliases.get(key);
							for (int p = 0; p < allExpr.size(); p++) {
								joinCondition.add(allExpr.get(p));
							}
						}
					}

					Operator temp = createJoinOp(combinedName, prev, first, joinCondition);
					prev = temp;
				}
			}
			child = prev;
			joinUsed = true;
		}

		// Join tables handled the select and the scans for all of their own tables.
		if (!joinUsed) {
			// Make the scan operator since we will always need it
			ScanOperator scan = new ScanOperator(fromTable);
			// Then check if there is some where condition. If there is then we need to make
			// the select
			if (where != null) {
				SelectOperator selectop = new SelectOperator(scan, where);
				child = selectop;
			} else {
				child = scan;
			}
		}

		// Check if there is a projection. If there is a projection then make that the
		// root node
		if (!(allColumns.get(0) instanceof AllColumns)) {
			ProjectOperator project = new ProjectOperator(child, allColumns);
			child = project;
		}

		// Check if there is a distinct
		if (distinct != null) {
			// For distinct there is always a order by element
			Operator sort = createSortOp(child, groupByElements);
			DuplicateEliminationOperator dup = new DuplicateEliminationOperator(sort);
			child = dup;
		} else if (groupByElements != null) {
			Operator sort = createSortOp(child, groupByElements);
			child = sort;
		}

		this.rootOperator = child;
	}

	/**
	 * Constructor for mapping the logical plan to the physical plan.
	 *
	 * @param rootOperator
	 */
	public QueryPlan(Operator rootOperator) {
		this.rootOperator = rootOperator;
	}

	/**
	 * Creates a join operator based on the configuration file.
	 *
	 * @param tableNames     the names of all joined tables
	 * @param leftOp         the left child operator
	 * @param rightOp        the right child operator
	 * @param joinConditions the conditions to join on
	 * @return a JoinOperator, one of TNLJ, BNLJ, or SMJ
	 */
	public Operator createJoinOp(String tableNames, Operator leftOp, Operator rightOp,
			ArrayList<Expression> joinConditions) {
		if (DatabaseCatalog.getInstance().getJoinMethod() == 0) { // Tuple nested loop join
			return new TNLJOperator(tableNames, leftOp, rightOp, joinConditions);
		} else if (DatabaseCatalog.getInstance().getJoinMethod() == 1) { // Block nested loop join
			return new BNLJOperator(tableNames,leftOp,rightOp,joinConditions,DatabaseCatalog.getInstance().getJoinPages());
		} else { // Sort merge join
			return new SMJOperator(tableNames, leftOp, rightOp, joinConditions);
		}
	}

	/**
	 * Creates a sort operator based on the configuration file.
	 *
	 * @param child  the child operator
	 * @param orders the order of columns to sort by
	 * @return
	 */
	public Operator createSortOp(Operator child, List orders) {
		if (DatabaseCatalog.getInstance().getSortMethod() == 0) { // in-memory sort
			return new SortOperator(child, orders);
		}
		// else use external sort
		return new ExternalSortOperator(child, orders,DatabaseCatalog.getInstance().getSortPages());
	}

	/**
	 * Retrieves the root operator.
	 *
	 * @return The root operator.
	 */
	public Operator getOperator() {
		return rootOperator;
	}
	

}
