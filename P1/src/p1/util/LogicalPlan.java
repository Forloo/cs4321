package p1.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.logicaloperator.LogicalAllJoin;
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalOperator;
import p1.logicaloperator.LogicalProject;
import p1.logicaloperator.LogicalScan;
import p1.logicaloperator.LogicalSort;
import p1.logicaloperator.LogicalUnique;
import p1.unionfind.UnionFind;
import p1.unionfind.UnionFindElement;
import p1.unionfind.UnionFindVisitor;

public class LogicalPlan {

	// Make a tree for the logical plan

	// Priority for specific elements
	// 1. Distinct
	// 2. Sorting operator
	// 3. Projection
	// 4. Join
	// 5. Selection
	// 6. Scan operator

	// The root operator
	private LogicalOperator rootOperator;

	/**
	 * The constructor for our logicalplan
	 *
	 * @param query The input query
	 */
	public LogicalPlan(Statement query) {
		// No reason to use a tree if we are just using the one to one mapping
		Select select = (Select) query;

		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

		List allColumns = plainSelect.getSelectItems();

		FromItem from = plainSelect.getFromItem();

		Expression where = plainSelect.getWhere();

		List joins = plainSelect.getJoins();

		Distinct distinct = plainSelect.getDistinct();

		List groupByElements = plainSelect.getOrderByElements();
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
		
		UnionFindVisitor testing= new UnionFindVisitor(where);
		if(where!=null) {
			where.accept(testing);
		}
		UnionFind findings= testing.getUnionFind();
		ArrayList<Expression> notUsed= testing.getnotUsableExpression();
		
		// All table names
		List<String> allTableNames = new ArrayList<String>();
		allTableNames.add(from.toString());

		if (joins != null) {
			for (int i = 0; i < joins.size(); i++) {
				allTableNames.add(joins.get(i).toString());
			}
		}

		// This will only be used if the join table is not null meaning that
		// there is more than one table that we are joining on.
		
		List<LogicalOperator> allTableOperators= null;
		if (joins!=null) {
			allTableOperators= this.makeAllOperators(allTableNames, expressionInfo,notUsed,findings);
		}

		LogicalOperator child = null;
		boolean joinUsed = false;
		// Make a union find
		// From the unionfind we can see the eexpression that are not used
		
		// Then in the select expression we can check if a given table will need that unused 
		// expression or not by comparing the names.
		
		
		// The join methods can still be assigned using the other method that we used befoore.
		
//		 If the joins table is not null then we need to make the new join operator
//		if (joins!=null) {
//			LogicalAllJoin joining= new LogicalAllJoin(allTableNames,allTableOperators,expressionInfoAliases,findings);
//			joinUsed=true;
//			child=joining;
//		}
		
		// Testing here to see if the union find will work as we expect it to.
//		System.out.println("====================================");
//		UnionFindVisitor testing= new UnionFindVisitor(where);
//		if(where!=null) {
//			where.accept(testing);
//		}
//		UnionFind findings= testing.getUnionFind();
//		ArrayList<Expression> notUsed= testing.getnotUsableExpression();
//		System.out.println("Finding elements");
//		System.out.println(findings.getUnionElement().size());
//		System.out.println(findings);
//		System.out.println("Elements that are not used");
//		System.out.println(notUsed);
//		System.out.println("============================================");
		// We did not get a compile error for any of the queries that we tried to work
		// on now it just means that we need to to see if the numbers inside of the
		// unionfind are the right
		// values that we expect them to be.

//		 If the joins table is not null then we need to make the new join operator
		if (joins != null) {
			LogicalAllJoin joining = new LogicalAllJoin(allTableNames, allTableOperators, expressionInfoAliases,notUsed);
			// Set union find for LogicalAllJoin to print for the logical plan file
			joining.setUnionFind(findings);
			joinUsed = true;
			child = joining;
		}

		// Check if there is more than one table being used
//		if (joins != null) {
//			boolean fromUsed = false;
//			LogicalJoin prev = null;
//			// Iterate through the join tables and make the logical join
//			for (int i = 0; i < joins.size(); i++) {
//				String alias = Aliases.getAlias(joins.get(i).toString());
//				if (!fromUsed) {
//					// there is no where condition meaning that we can just make both of
//					// the operators just scan tables
//					if (expressionInfo == null) {
//						LogicalScan first = new LogicalScan(fromTable);
//						LogicalScan second = new LogicalScan(alias);
//						String combinedName = fromTable + "," + alias;
//						LogicalJoin temp = new LogicalJoin(combinedName, first, second, null);
//						prev = temp;
//					} else {
//						// Get the where conditions for all of the tables and assign them accordingly
//						LogicalOperator first = null;
//						if (expressionInfo.containsKey(fromTable)) {
//							ArrayList<Expression> conditions = expressionInfo.get(fromTable);
//							LogicalScan scanone = new LogicalScan(fromTable);
//							LogicalFilter selectone = new LogicalFilter(scanone, conditions.get(0));
//							first = selectone;
//						} else {
//							LogicalScan scanone = new LogicalScan(fromTable);
//							first = scanone;
//						}
//
//						// Do the same for the join table
//						LogicalOperator second = null;
//						if (expressionInfo.containsKey(alias)) {
//							// Get the arraylist of conditions
//							ArrayList<Expression> conditions2 = expressionInfo.get(alias);
//							LogicalScan scantwo = new LogicalScan(alias);
//							LogicalFilter selecttwo = new LogicalFilter(scantwo, conditions2.get(0));
//							second = selecttwo;
//						} else {
//							LogicalScan scantwo = new LogicalScan(Aliases.getAlias(joins.get(0).toString()));
//							second = scantwo;
//						}
//
//						String combinedName = fromTable + "," + alias;
//						String[] tablesNeeded = combinedName.split(",");
//						Arrays.sort(tablesNeeded);
//						String sortedTablesNeeded = String.join(",", tablesNeeded);
//
//						// Check if there is match for this table. If there is a match
//						// then assign the conditions in that arraylist to the join
////						ArrayList<Expression> joinCondition = null;
////						if (expressionInfo.containsKey(sortedTablesNeeded)) {
////							ArrayList<Expression> joinConditions = expressionInfo.get(sortedTablesNeeded);
////							joinCondition = joinConditions;
////						}
//						// Appears that the second way of doing the join conditions is better
//						// this gets all of the conditions no matter even if there are duplicates in the problem.
//						ArrayList<Expression> joinCondition = new ArrayList<Expression>();
//						HashSet<String> tblsNeeded = new HashSet<String>();
//						for(int n=0;n<tablesNeeded.length;n++) {
//							tblsNeeded.add(tablesNeeded[n]);
//						}
//						
//						for(String [] key: expressionInfoAliases.keySet()) {
//							boolean allIncluded = true;
//							for (int l = 0; l < key.length; l++) {
//								allIncluded = allIncluded && tblsNeeded.contains(key[l]);
//							}
//
//							if (allIncluded) {
////								System.out.println("We entered this loop and some conditions were assigned");
//								ArrayList<Expression> allExpr = expressionInfoAliases.get(key);
//								for (int p = 0; p < allExpr.size(); p++) {
//									joinCondition.add(allExpr.get(p));
//								}
//							}
//						}
//						
//						System.out.println(joinCondition); 
//						LogicalJoin temp = new LogicalJoin(combinedName, first, second, joinCondition);
//						prev = temp;
//					}
//					fromUsed = true;
//				} else // The from table was used meaning that the prev node is not null
//				// so the left child will be a join operator node.
//				if (expressionInfo == null) {
//					LogicalScan first = new LogicalScan(alias);
//					String combinedName = prev.getTables() + "," + alias;
//					LogicalJoin temp = new LogicalJoin(combinedName, prev, first, null);
//					prev = temp;
//				} else {
//					LogicalOperator first = null;
//					// Check if we should put any conditions on the right table
//					if (expressionInfo.containsKey(alias)) {
//						ArrayList<Expression> conditions = expressionInfo.get(alias);
//						LogicalScan scanone = new LogicalScan(alias);
//						LogicalFilter selectone = new LogicalFilter(scanone, conditions.get(0));
//						first = selectone;
//					} else {
//						// There are no conditions for the right table so the right is just a scan
//						LogicalScan scanone = new LogicalScan(alias);
//						first = scanone;
//					}
//
//					String combinedName = prev.getTables() + "," + alias;
//					ArrayList<Expression> joinCondition = new ArrayList<Expression>();
//					String[] splitted = combinedName.split(",");
//					HashSet<String> tblsNeed = new HashSet<String>();
//					for (int k = 0; k < splitted.length; k++) {
//						tblsNeed.add(splitted[k]);
//					}
//					
//					for (String[] key : expressionInfoAliases.keySet()) {
//						boolean allIncluded = true;
//						for (int l = 0; l < key.length; l++) {
//							allIncluded = allIncluded && tblsNeed.contains(key[l]);
//						}
//
//						if (allIncluded) {
////							System.out.println("We entered this loop and some conditions were assigned");
//							ArrayList<Expression> allExpr = expressionInfoAliases.get(key);
//							for (int p = 0; p < allExpr.size(); p++) {
//								joinCondition.add(allExpr.get(p));
//							}
//						}
//					}
//					
////					System.out.println("=========================");
////					System.out.println(joinCondition);
////					System.out.println("+++++++++++++++++++++++++++++");
//					LogicalJoin temp = new LogicalJoin(combinedName, prev, first, joinCondition);
//					prev = temp;
//
//				}
//			}
//			child = prev;
//			joinUsed = true;
//		}

		if (!joinUsed) {
			// We need to make the scan table operator anyway
			LogicalScan logicalscan = new LogicalScan(fromTable);

			// Check if there is a where clause
			if (where != null) {
				ArrayList<Expression> allExpr= new ArrayList<Expression>();
				allExpr.add(where);
				LogicalFilter filter = new LogicalFilter(logicalscan, allExpr,findings.getUnionElement());
				child = filter;
			} else {
				child = logicalscan;
			}

		}

		// Check if there is projection node that is needed here
		if (!(allColumns.get(0) instanceof AllColumns)) {
			LogicalProject project = new LogicalProject(child, allColumns);
			child = project;
		}

		// Check if there is a distinct
		if (distinct != null) {
			// For distinct there is always a order by element
			LogicalSort sort = new LogicalSort(child, groupByElements);
			LogicalUnique dup = new LogicalUnique(sort);
			child = dup;
		} else if (groupByElements != null) {
			LogicalSort sort = new LogicalSort(child, groupByElements);
			child = sort;
		}

		this.rootOperator = child;

	}

	/**
	 * Constructs logicalOperators for all tables
	 * 
	 * @param allTableNames   : The string name of all tables
	 * @param tableConditions : Contains all conditions for specific tables.
	 * @return List<LogicalOperator> for each table.
	 */
	private List<LogicalOperator> makeAllOperators(List<String> allTableNames,HashMap<String,ArrayList<Expression>> tableConditions, ArrayList<Expression> notUsed, UnionFind uf){
		List<LogicalOperator> ret= new ArrayList<LogicalOperator>();
		
		// Iterate through for each table and determine whether it needs a scan operator or 
		// a filter operation.
		for (int i = 0; i < allTableNames.size(); i++) {
			String alias = Aliases.getAlias(allTableNames.get(i));
			if (tableConditions == null) {
				LogicalScan currOp = new LogicalScan(alias);
				ret.add(currOp);
			} else {
				if (tableConditions.containsKey(alias)) {
					ArrayList<Expression> currConditions= tableConditions.get(alias);
					
					ArrayList<Expression> notIncluded = new ArrayList<Expression>();
					
					for(int k=0;k<currConditions.size();k++) {
						Expression currExpression= currConditions.get(k);
						boolean notApplied=false;
						if(currExpression instanceof EqualsTo) {
							EqualsTo changed= (EqualsTo) currExpression;
							Expression left = changed.getLeftExpression();
							String leftAttr= left.toString();
							ArrayList<UnionFindElement> allElements= uf.getUnionElement();
							for(int b=0;b<allElements.size();b++) {
								ArrayList<String> attributes= allElements.get(b).getAttributeSet();
								if (attributes.contains(leftAttr)) {
									if(allElements.get(b).getMinValue()==Integer.MIN_VALUE && allElements.get(b).getMaxValue()==Integer.MAX_VALUE) {
										notApplied=true;
									}
								}
							}
							
						}
						if(notUsed.contains(currExpression) || notApplied) {
							notIncluded.add(currExpression);
						}
					}
					
//					System.out.println("This is the beginning delimiter");
//					System.out.println(currConditions);
//					System.out.println(notIncluded);
//					System.out.println(notUsed);
					
					LogicalScan scanOp = new LogicalScan(alias);
//					ArrayList<UnionFindElement> allElements= uf.getUnionElement();
					
					
//					System.out.println("This is the end delimiter");
					// Make the scan operator for this class.
					LogicalFilter currOp = new LogicalFilter(scanOp, notIncluded,uf.getUnionElement());
					ret.add(currOp);
				} else {
					LogicalScan currOp = new LogicalScan(alias);
					ret.add(currOp);
				}
			}
		}

		return ret;
	}
	

	private LogicalAllJoin makeOperations(List<String> tableNames, List<LogicalOperator> tableOperations) {
		return null;
	}

	/**
	 * Retrieves the rootOperator
	 *
	 * @return A logicalOperator determining the root of the current node.
	 */
	public LogicalOperator getOperator() {
		return rootOperator;
	}

	/**
	 * Accepts a physicalplanbuilder object and then uses it to build the physical
	 * tree.
	 *
	 * @param pb
	 */
	public void accept(PhysicalPlanBuilder pb) {
		pb.visit(this);
	}
}
