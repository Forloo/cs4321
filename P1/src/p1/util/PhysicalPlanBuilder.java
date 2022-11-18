package p1.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SubSelect;
import p1.logicaloperator.LogicalAllJoin;
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalJoin;
import p1.logicaloperator.LogicalOperator;
import p1.logicaloperator.LogicalProject;
import p1.logicaloperator.LogicalScan;
import p1.logicaloperator.LogicalSort;
import p1.logicaloperator.LogicalUnique;
import p1.operator.BNLJOperator;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.ExternalSortOperator;
import p1.operator.IndexScanOperator;
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.SMJOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.TNLJOperator;
import p1.unionfind.UnionFind;
import p1.unionfind.UnionFindElement;

/**
 * Walks through logical plan and builds a physical plan
 */
public class PhysicalPlanBuilder implements ExpressionVisitor {

	// The physicalplan object
	private QueryPlan physicalPlan;
	// The plainselect containing the query information
	private Statement query;

	/**
	 * The constructor for the PhysicalPlanBuilder
	 *
	 * @param plainSelect
	 */
	public PhysicalPlanBuilder(Statement query) {
		physicalPlan = null;
		this.query = query;
	}

	/**
	 * Get the query information
	 *
	 * @return A plainSelect representing the query
	 */
	public Statement getQuery() {
		return query;
	}

	/**
	 * Retrieves the converted logical plan
	 *
	 * @return A querytree containing the
	 */
	public QueryPlan getPlan() {
		return physicalPlan;
	}

	/**
	 * Set the query plan
	 *
	 * @param plan the query plan to replace the current plan with
	 */
	private void setPlan(QueryPlan plan) {
		physicalPlan = plan;
	}

	/**
	 * Checks if a string is an integer.
	 * 
	 * @param s the string to check
	 * @return true if s is an integer, false if not
	 */
	private boolean isInt(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Generates the physical tree given the root node for the Logicaltree After we
	 * get more than one join working we will also pass into this an object telling
	 * us the configuration of what joins that we want.
	 *
	 * @param rootOperator The root operator from the logical tree
	 * @return An operator for the root node for the logical tree.
	 */
	public Operator generatePhysicalTree(LogicalOperator rootOperator) {

		// Logical scan base case check
		if (rootOperator instanceof LogicalScan) {
			// Cast the rootOperator to the logical scan and then get the field that we want
			LogicalScan cpy = (LogicalScan) rootOperator;
			// Make this into the physicalOperator
			return new ScanOperator(cpy.getFromTable());
		}

		if (rootOperator instanceof LogicalFilter) {
			// Cast the rootoperator to the logical filter
			LogicalFilter cpy = (LogicalFilter) rootOperator;

			Operator child = generatePhysicalTree(cpy.getChild());

			// TODO: P4
			if (true) {
				String childTable = Aliases.getTable(child.getTable());

				// TODO: P4 DECIDE WHICH INDEX TO USE IF MULTIPLE INDEXES FOR ONE TABLE
				for (String col : DatabaseCatalog.getInstance().getIndexInfo().keySet()) { // THIS FOR LOOP IS A
																							// PLACEHOLDER; DELETE WHEN
																							// DONE WITH SECTION 3.3
					if (col.contains(childTable)) {
						childTable = col; // childTable is now tableName + "." + colName
					}
				}
				String idxCol = childTable.substring(childTable.indexOf(".") + 1);
//				String[] exps = cpy.getExpression().toString().split(" AND ");
//				System.out.println(cpy.getExpression());
				if (cpy.getExpression().size() > 0) {
					int lowkey = Integer.MIN_VALUE;
					int highkey = Integer.MAX_VALUE;
					for (int k = 0; k < cpy.getExpression().size(); k++) {
						String[] exps = cpy.getExpression().get(k).toString().split(" AND ");
						for (String e : exps) {
							String[] exp = e.split(" ");
							String[] left = exp[0].split("\\.");
							String[] right = exp[2].split("\\.");
							if ((left.length > 1 && left[1].equals(idxCol) && isInt(right[0]))
									|| (right.length > 1 && right[1].equals(idxCol) && isInt(left[0]))) {
								String comparator = exp[1];
								if (isInt(right[0])) {
									if (comparator.equals("<")) {
										highkey = Math.min(Integer.parseInt(right[0]) - 1, highkey);
									} else if (comparator.equals("<=")) {
										highkey = Math.min(Integer.parseInt(right[0]), highkey);
									} else if (comparator.equals(">")) {
										lowkey = Math.max(Integer.parseInt(right[0]) + 1, lowkey);
									} else if (comparator.equals(">=")) {
										lowkey = Math.max(Integer.parseInt(right[0]), lowkey);
									} else if (comparator.equals("=")) {
										lowkey = Math.max(Integer.parseInt(right[0]), lowkey);
										highkey = Math.min(Integer.parseInt(right[0]), highkey);
									}
								} else {
									if (comparator.equals("<")) {
										lowkey = Math.max(Integer.parseInt(left[0]) + 1, lowkey);
									} else if (comparator.equals("<=")) {
										lowkey = Math.max(Integer.parseInt(left[0]), lowkey);
									} else if (comparator.equals(">")) {
										highkey = Math.min(Integer.parseInt(left[0]) - 1, highkey);
									} else if (comparator.equals(">=")) {
										highkey = Math.min(Integer.parseInt(left[0]), highkey);
									} else if (comparator.equals("=")) {
										lowkey = Math.max(Integer.parseInt(left[0]), lowkey);
										highkey = Math.min(Integer.parseInt(left[0]), highkey);
									}
								}
							}
						}
					}
					String[] exps = cpy.getExpression().get(0).toString().split("AND");
//					int lowkey = Integer.MIN_VALUE;
//					int highkey = Integer.MAX_VALUE;
//					for (String e : exps) {
//						String[] exp = e.split(" ");
//						String[] left = exp[0].split("\\.");
//						String[] right = exp[2].split("\\.");
//						if ((left.length > 1 && left[1].equals(idxCol) && isInt(right[0]))
//								|| (right.length > 1 && right[1].equals(idxCol) && isInt(left[0]))) {
//							String comparator = exp[1];
//							if (isInt(right[0])) {
//								if (comparator.equals("<")) {
//									highkey = Math.min(Integer.parseInt(right[0]) - 1, highkey);
//								} else if (comparator.equals("<=")) {
//									highkey = Math.min(Integer.parseInt(right[0]), highkey);
//								} else if (comparator.equals(">")) {
//									lowkey = Math.max(Integer.parseInt(right[0]) + 1, lowkey);
//								} else if (comparator.equals(">=")) {
//									lowkey = Math.max(Integer.parseInt(right[0]), lowkey);
//								} else if (comparator.equals("=")) {
//									lowkey = Math.max(Integer.parseInt(right[0]), lowkey);
//									highkey = Math.min(Integer.parseInt(right[0]), highkey);
//								}
//							} else {
//								if (comparator.equals("<")) {
//									lowkey = Math.max(Integer.parseInt(left[0]) + 1, lowkey);
//								} else if (comparator.equals("<=")) {
//									lowkey = Math.max(Integer.parseInt(left[0]), lowkey);
//								} else if (comparator.equals(">")) {
//									highkey = Math.min(Integer.parseInt(left[0]) - 1, highkey);
//								} else if (comparator.equals(">=")) {
//									highkey = Math.min(Integer.parseInt(left[0]), highkey);
//								} else if (comparator.equals("=")) {
//									lowkey = Math.max(Integer.parseInt(left[0]), lowkey);
//									highkey = Math.min(Integer.parseInt(left[0]), highkey);
//								}
//							}
//						}
//					}
					Integer high = highkey < Integer.MAX_VALUE ? highkey : null;
					Integer low = lowkey > Integer.MIN_VALUE ? lowkey : null;
					// Assuming inclusive keys
//					System.out.println(childTable);
					if (high != null || low != null) {
						String[] indexInfo = DatabaseCatalog.getInstance().getIndexInfo().get(childTable);
						boolean clustered = indexInfo[0].equals("1") ? true : false;
						int indexIdx = DatabaseCatalog.getInstance().getSchema().get(Aliases.getTable(child.getTable()))
								.indexOf(childTable);
						String idxFile = DatabaseCatalog.getInstance().getIndexDir() + childTable;
						child = new IndexScanOperator(child.getTable(), low, high, clustered, indexIdx, idxFile);
					}
				}
			}
			HashMap<String, ArrayList<Integer>> ufRestraints = new HashMap<String, ArrayList<Integer>>();
			// The child table is always a scan child so we can just convert that child into
			// a scanOperator
			// Then from that we can get the schema from that and using the schema then we
			// can assign the right conditions
			// for that given table.
			ScanOperator childOp = (ScanOperator) child;
			ArrayList<String> schema = childOp.getSchema();
			ArrayList<UnionFindElement> ufInfo = cpy.getUfRestraints();

			for (int k = 0; k < ufInfo.size(); k++) {
				// For each of the attribute constraints if that string is in our attribute
				// table then we add it to the list of constraints.
				UnionFindElement curr = ufInfo.get(k);
				// Get the arrayList of attributes in this current union set
				ArrayList<String> attributes = curr.getAttributeSet();
				for (int l = 0; l < attributes.size(); l++) {
					if (schema.contains(attributes.get(l))) {
						ArrayList<Integer> bounds = new ArrayList<Integer>();
						bounds.add(curr.getMinValue());
						bounds.add(curr.getMaxValue());
						ufRestraints.put(attributes.get(l), bounds);
					}
				}
			}
//			System.out.println("Delimit this section from the top");
			// Print out the table that we are getting the restraints on
//			System.out.println(child.getTable());

			// Print out the restraints that we are assigning to this table
//			for(String key: ufRestraints.keySet()) {
//				System.out.println(key);
//				System.out.println(ufRestraints.get(key));
//			}
//			System.out.println("Delimit this value from the bottom");

			// It looks like adding the bounds to the value work so the next step in doing
			// this
			// is to integrate with the rest of the code.

//			System.out.println(child.getTable());
//			System.out.println(cpy.getExpression());
//			System.out.println("delimiter is being set here: the delimiter that is being set here is just this");
			return new SelectOperator(child, cpy.getExpression(), ufRestraints);
		}

		if (rootOperator instanceof LogicalProject) {
			// Cast the element to the logical project
			LogicalProject cpy = (LogicalProject) rootOperator;

			// Get the child element for this
			Operator child = generatePhysicalTree(cpy.getChild());

			ProjectOperator project = new ProjectOperator(child, cpy.getSelects());

			return project;
		}

		// If the rootOperator is our new LogicalAllJoin then the only thing
		// that get the list of all operators for each of those tables
		// Then based on some test statistics function we will choose which join that we
		// want to
		// make for each of those table joins that we have to do.
		if (rootOperator instanceof LogicalAllJoin) {
			Operator prevJoin = null;
			LogicalAllJoin cpy = (LogicalAllJoin) rootOperator;
			ArrayList<Expression> notUsed = cpy.getUnusedOperators();
			UnionFind uf = cpy.getUnionFind();
//			System.out.println(cpy.getUnionFind());
//			System.out.println("Union find element found");
			// Get the list of all the tables
			List<String> allTables = cpy.getTableNames();
			List<LogicalOperator> operators = cpy.getTableOperators();
			HashMap<String[], ArrayList<Expression>> allConditions = cpy.getConditions();
//			System.out.println("Delimit this value up on the top here");
//			System.out.println("All conditions are in here");
//			for(String [] key: allConditions.keySet()) {
//				System.out.println(allConditions.get(key));
//			}
//			System.out.println("All conditions ended in the section before us");
//			System.out.println(notUsed);
//			System.out.println("Delimit this value right here");
			// If we made a logicalalljoin then there is at least two tables
			for (int i = 1; i < allTables.size(); i++) {
				// The first join creation always joins two different tables
				if (i == 1) {
					// Write a function here to generate the conditions for the join operator

					// First thing to do is convert the two logical operator into the right
					// physical Operator.
					Operator left = generatePhysicalTree(operators.get(i - 1));
					Operator right = generatePhysicalTree(operators.get(i));
//					System.out.println("=================================");
					ArrayList<Expression> joinConditions = this.getJoinConditions(left, right, allConditions, notUsed,
							uf);
					String joinName = left.getTable() + "," + right.getTable();
//					System.out.println(joinName);
//					System.out.println("++++++++++++++++++++++++++++++++++++");
					Operator joinElement = this.chooseJoin(joinName, left, right, joinConditions);
//					System.out.println(joinName);
//					System.out.println(joinConditions);
					prevJoin = joinElement;
				} else {
					// If not the first element then there must be anohter join before this.
					Operator left = prevJoin;
					Operator right = generatePhysicalTree(operators.get(i));
					String joinName = left.getTable() + "," + right.getTable();
//					System.out.println("=================================");
					ArrayList<Expression> joinConditions = this.getJoinConditions(left, right, allConditions, notUsed,
							uf);
					System.out.println(joinName);
//					System.out.println("++++++++++++++++++++++++++++++++++++++");
					Operator joinElement = this.chooseJoin(joinName, left, right, joinConditions);
					prevJoin = joinElement;
//					System.out.println(joinName);
//					System.out.println(joinConditions);
				}
//				System.out.println("+++++++++++++++++++");
			}

			return prevJoin;
		}

		if (rootOperator instanceof LogicalJoin) {
			// Cast the element to the logical join
			LogicalJoin cpy = (LogicalJoin) rootOperator;

			// Get the left and right child
			Operator leftchild = generatePhysicalTree(cpy.getLeftChild());
			Operator rightchild = generatePhysicalTree(cpy.getRightChild());

			// TODO: P4
			if (true) { // Tuple nested loop join
				return new TNLJOperator(cpy.getTables(), leftchild, rightchild, cpy.getExpression());
			} else if (true) { // Block nested loop join
				return new BNLJOperator(cpy.getTables(), leftchild, rightchild, cpy.getExpression(), 4);
			} else { // Sort merge join
				return new SMJOperator(cpy.getTables(), leftchild, rightchild, cpy.getExpression());
			}
		}

		if (rootOperator instanceof LogicalSort) {
			// Cast the element to a logical sort
			LogicalSort cpy = (LogicalSort) rootOperator;

			// Get the child for the sort
			Operator child = generatePhysicalTree(cpy.getChild());

			Operator sort;
			ArrayList<String> orderBy = new ArrayList<String>();
			if (cpy.getOrderBy() != null) {
				for (Object el : cpy.getOrderBy()) {
					orderBy.add(el.toString());
				}
			}

			return new ExternalSortOperator(child, orderBy, 4, DatabaseCatalog.getInstance().getTempDir(), 0);
		}

		if (rootOperator instanceof LogicalUnique) {
			// Cast it to the logicalunique
			LogicalUnique cpy = (LogicalUnique) rootOperator;

			// Get the child node which we know is a sort
			Operator child = generatePhysicalTree(cpy.getChild());

			// Make the physical elimination operator
			DuplicateEliminationOperator dup = new DuplicateEliminationOperator(child);

			return dup;
		}

		// Reaching this is not possible
		return null;

	}

	public void visit(LogicalPlan lp) {
		// Get the rootOperator for the tree
		LogicalOperator root = lp.getOperator();
		Operator physicalroot = this.generatePhysicalTree(root);
		QueryPlan physicalcopy = new QueryPlan(physicalroot);
		this.physicalPlan = physicalcopy;
	}

	private Operator chooseJoin(String tableNames, Operator left, Operator right,
			ArrayList<Expression> joinConditions) {

		Operator result = null;
		if (true) {
			result = new TNLJOperator(tableNames, left, right, joinConditions);
		} else if (true) {
			result = new BNLJOperator(tableNames, left, right, joinConditions, 10);
		} else {
			// Change this back later for some reason there is an error when we change this
			// from the tuple nest loop join to the sort merge join
			result = new SMJOperator(tableNames, left, right, joinConditions);
		}

		return result;
	}

	private ArrayList<Expression> getJoinConditions(Operator left, Operator right,
			HashMap<String[], ArrayList<Expression>> conditions, ArrayList<Expression> notUsed, UnionFind uf) {
		String leftName = left.getTable();
		String rightName = right.getTable();
		String combinedName = leftName + "," + rightName;
		String[] tablesNeeded = combinedName.split(",");
		HashSet<String> tblsNeeded = new HashSet<String>();
		ArrayList<Expression> joinCondition = new ArrayList<Expression>();
		// Get all of the tables for the current join making sure that there are no
		// duplicates.
		ArrayList<Expression> filteredConditions = new ArrayList<Expression>();
		for (int i = 0; i < tablesNeeded.length; i++) {
			tblsNeeded.add(tablesNeeded[i]);
		}

		for (String[] key : conditions.keySet()) {
			boolean allIncluded = true;
			for (int l = 0; l < key.length; l++) {
				allIncluded = allIncluded && tblsNeeded.contains(key[l]);
			}

			if (allIncluded) {
//				System.out.println("We entered this loop and some conditions were assigned");
				ArrayList<Expression> allExpr = conditions.get(key);
				for (int p = 0; p < allExpr.size(); p++) {
//					System.out.println(allExpr.get(p));
//					System.out.println(allExpr.get(p) instanceof EqualsTo);
					joinCondition.add(allExpr.get(p));
					if (notUsed.contains(allExpr.get(p))) {
						filteredConditions.add(allExpr.get(p));
					} else {
						// Check if this an equality operator since that is the only operator
						// that needs to be checked
						if (allExpr.get(p) instanceof EqualsTo) {
							// Get the left expression and then make it into a string
							// it must be in the unionfind since it is not in the list of
							// expressions that we are parsing.
							// For some of the queries that we have they are not valid since we have
							// the form that A1=v1, A2=v2, and A1=A2 but then the values cannot be equal
							// which they said is not allowed and not tested.
							EqualsTo leftExpression = (EqualsTo) allExpr.get(p);
							Expression leftAttribute = leftExpression.getLeftExpression();
							String leftAttributeValue = leftAttribute.toString();
							UnionFindElement ufe = uf.find(leftAttributeValue);
							if (ufe.getMaxValue() == Integer.MAX_VALUE && ufe.getMinValue() == Integer.MIN_VALUE) {
								filteredConditions.add(allExpr.get(p));
//								System.out.println("Did we enter inside of this loop");
							}
						}
					}
				}
			}
		}
//		System.out.println(joinCondition);
//		System.out.println(filteredConditions);

		return filteredConditions;
	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}

}