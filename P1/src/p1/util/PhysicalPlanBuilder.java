package p1.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import p1.dp.JoinDp;
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

	//this is needed to pass in dpJoin to calculate min cost join order
	private HashMap<String, int[]> dbStatsInfo;
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

//		// Logical scan base case check
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
			
// BEGIN SECTION 3.3 

		if (true) {
			String childTable = Aliases.getTable(child.getTable());
				
			String idxCol = childTable.substring(childTable.indexOf(".") + 1);

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

				Integer high = highkey < Integer.MAX_VALUE ? highkey : null;
				Integer low = lowkey > Integer.MIN_VALUE ? lowkey : null;	
				
				DatabaseCatalog db = DatabaseCatalog.getInstance();
				HashMap<String, int[]> stats = db.statsInfo;
				int numTuples = stats.get(childTable)[0]; // t
				

				ArrayList<String> attr = child.getSchema();
					
				double scanCost = Math.ceil(numTuples * (4 * attr.size()) / 4096); 
			
				double minCost = scanCost; // default					
				double reductionFactor = 1.0; // r					
				int numPages = (int) scanCost; // p 
				
				Set<String> keySet = DatabaseCatalog.getInstance().getIndexInfo().keySet(); 

				Iterator<String> itr = keySet.iterator();
				ArrayList<String> indexNames = new ArrayList<String>();
				
				// need names of each index, not just name of table 
				while (itr.hasNext()) {
				    String key = itr.next();
				    indexNames.add(key);
				}				
								
				// check that there is an index to use 
				if(db.getIndexInfo().size()!= 0 && indexNames != null) {
																		
					String finalIndex = null;
					int counter = 0; // to keep track of colIdx
					int colIdx = 0;
						
					ArrayList<Integer> leaves = DatabaseCatalog.getInstance().getNumLeaves();
					
					boolean clustered = true;
																		
					// calculate index scan cost for each index 
					for (String columnName : indexNames) {												
						clustered = indexNames.get(counter).equals("1") ? true : false;							
						
						int[] range = DatabaseCatalog.getInstance().statsInfo.get(columnName);
																			
						double total = range[1] - range[0]; // total range of values for this attribute 			    		
						double min = range[0]; 
				    	double max = range[1]; 				    	
				    		
				    	if (high != null) max = high; 	    		
				    	if (low != null) min = low; 
				    					    		
				   		if (low == null && high != null) {
				   			reductionFactor =  (high - min) / total;
				   		} else if (low == null && high == null) {
				   			reductionFactor = 1.0;
				   		} else if (low != null && high == null) {
				   			reductionFactor = (max - low) / total;
				   		} else if (reductionFactor <= 0) {
				   			reductionFactor = 1.0;
				   		} else {
					   		reductionFactor = (max - min) / total; 
				    	}			    						   
				    		
						int numLeaves = leaves.get(counter); // l
							
						double indexCost;

						if (clustered) {
							indexCost = 3 + numPages * reductionFactor;
						} else {
							indexCost = 3 + numLeaves * reductionFactor + numTuples * reductionFactor;
						}
																			
						if (indexCost < minCost) {
							minCost = indexCost; 
							finalIndex = columnName; // index to use, return null if no index should be used (scan instead)
							colIdx = counter; // the column index that the table is indexed on
						} 
						counter++; 

					} 
					
						
					if (finalIndex == null) {
						;	
							
					} else {
						// make index scan operator on finalIndex 
//						if (high != null || low != null) {
//						System.out.println("entered the second index loop");
						String idxFile = DatabaseCatalog.getInstance().getIndexDir() + finalIndex;
//						System.out.println("Using the index scan operator in this part");
						child = new IndexScanOperator(child.getTable(), low, high, clustered, colIdx, idxFile);
//						}
//						else return new ScanOperator(copy.getFromTable()); // if lowkey and highkey are null then we scan whole table
					}
				}
					
			    
				
// END SECTION 3.3 
					
//				// TODO: P4 DECIDE WHICH INDEX TO USE IF MULTIPLE INDEXES FOR ONE TABLE
//				for (String col : DatabaseCatalog.getInstance().getIndexInfo().keySet()) { // THIS FOR LOOP IS A
//																								// PLACEHOLDER; DELETE WHEN
//																								// DONE WITH SECTION 3.3
//					if (col.contains(childTable)) {
//						childTable = col; // childTable is now tableName + "." + colName
//					}
//				}
		}
		}
		HashMap<String, ArrayList<Integer>> ufRestraints = new HashMap<String, ArrayList<Integer>>();
		// The child table is always a scan child so we can just convert that child into
		// a scanOperator
		// Use the schema to assign the right conditions
		ScanOperator childOp = (ScanOperator) child;
		ArrayList<String> schema = childOp.getSchema();
		ArrayList<UnionFindElement> ufInfo = cpy.getUfRestraints();

		for (int k = 0; k < ufInfo.size(); k++) {
			// Check attribute constraint. Match with table then add
			UnionFindElement curr = ufInfo.get(k);
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
		
		cpy.setRelevantConstraints(ufRestraints);
//		System.out.println("Something ended up being returned from this function");
		return new SelectOperator(child, cpy.getExpression(),ufRestraints);
		}

		if (rootOperator instanceof LogicalProject) {
			// Cast the element to the logical project
			LogicalProject cpy = (LogicalProject) rootOperator;

			// Get the child element for this
			Operator child2 = generatePhysicalTree(cpy.getChild());

			ProjectOperator project = new ProjectOperator(child2, cpy.getSelects());

			return project;
		}

		// TODO After using dynamic programming we will choose the order in which query 
		// objects will be joined together
		if (rootOperator instanceof LogicalAllJoin) {
			
			//call JoinDP here, get the order of tables to join
			
//			System.out.println("LOGICAL ALL JOIN!"); //PRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINT
			Operator prevJoin = null;

			LogicalAllJoin cpy1 = (LogicalAllJoin) rootOperator;
			
//			System.out.println(cpy.getConditions()); //PRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINT
		
			//JoinDp test = new JoinDp(cpy1,dbStatsInfo);//TESTINGTESTINGTESTINGTESTINGTESTINGTESTINGTESTINGTESTINGTESTINGTESTINGTESTING
			
			
			
			ArrayList<Expression> notUsed= cpy1.getUnusedOperators();
			UnionFind uf= cpy1.getUnionFind();
			List<String> allTables = cpy1.getTableNames();
			List<LogicalOperator> operators = cpy1.getTableOperators();
			HashMap<String[], ArrayList<Expression>> allConditions = cpy1.getConditions();
			
			ArrayList<Expression> usedExpression = new ArrayList<Expression>();
			HashSet<Expression> usedJoinExpression= new HashSet<Expression>();
			// At least two tables  
			for (int i = 1; i < allTables.size(); i++) {
				// The first join creation always joins two different tables
				if (i == 1) {
					// Write a function here to generate the conditions for the join operator

					// Convert to the correct physical operators
					Operator left = generatePhysicalTree(operators.get(i - 1));
//					System.out.println(operators.get(i-1));
//					System.out.println(operators.get(i));
//					System.out.println("Before getting the left operator");
//					System.out.println(left);
//					System.out.println("After getting the right operator");
					this.addExpressions(left, usedExpression);
					this.updateUsedJoinExpressions(usedExpression, usedJoinExpression);
					Operator right = generatePhysicalTree(operators.get(i));
					this.addExpressions(right, usedExpression);
					this.updateUsedJoinExpressions(usedExpression, usedJoinExpression);
					ArrayList<Expression> joinConditions = this.getJoinConditions(left, right, allConditions, notUsed,uf,usedJoinExpression);
					String joinName = left.getTable() + "," + right.getTable();
//					System.out.println("Join name: " + joinName);
//					System.out.println("++++++++++++++++++++++++++++++++++++");
					Operator joinElement = this.chooseJoin(joinName, left, right, joinConditions);
//					System.out.println(joinName);
//					System.out.println(joinConditions);
					prevJoin = joinElement;
				} else {
					Operator left = prevJoin;
					Operator right = generatePhysicalTree(operators.get(i));
					this.addExpressions(right, usedExpression);
					this.updateUsedJoinExpressions(usedExpression, usedJoinExpression);

					String joinName = left.getTable() + "," + right.getTable();

					ArrayList<Expression> joinConditions = this.getJoinConditions(left, right, allConditions,notUsed,uf,usedJoinExpression);

					Operator joinElement = this.chooseJoin(joinName, left, right, joinConditions);
					prevJoin = joinElement;
//					System.out.println(joinName);
//					System.out.println(joinConditions);
				}
//				System.out.println("+++++++++++++++++++");
			}
			
			HashMap<String[], ArrayList<Expression>> updatedConditions= this.updateConditions(allConditions, notUsed, uf.getUnionElement(), usedExpression);
			cpy1.setConditions(updatedConditions);
			
			return prevJoin;
		}

		if (rootOperator instanceof LogicalJoin) {
			// Cast the element to the logical join
			LogicalJoin copy = (LogicalJoin) rootOperator;

			// Get the left and right child
			Operator leftchild = generatePhysicalTree(copy.getLeftChild());
			Operator rightchild = generatePhysicalTree(copy.getRightChild());

			// TODO: P4
			if (true) { // Tuple nested loop join
				return new TNLJOperator(copy.getTables(), leftchild, rightchild, copy.getExpression());
			} else if (true) { // Block nested loop join
				return new BNLJOperator(copy.getTables(), leftchild, rightchild, copy.getExpression(), 4);
			} else { // Sort merge join
				return new SMJOperator(copy.getTables(), leftchild, rightchild, copy.getExpression());
			}
		}

		if (rootOperator instanceof LogicalSort) {
			// Cast the element to a logical sort
			LogicalSort copy = (LogicalSort) rootOperator;

			// Get the child for the sort
			Operator child2 = generatePhysicalTree(copy.getChild());

			Operator sort;
			ArrayList<String> orderBy = new ArrayList<String>();
			if (copy.getOrderBy() != null) {
				for (Object el : copy.getOrderBy()) {
					orderBy.add(el.toString());
				}
			}

			return new ExternalSortOperator(child2, orderBy, 4, DatabaseCatalog.getInstance().getTempDir(), 0);
		}

		if (rootOperator instanceof LogicalUnique) {
			// Cast it to the logicalunique
			LogicalUnique copy = (LogicalUnique) rootOperator;

			// Get the child node which we know is a sort
			Operator child2 = generatePhysicalTree(copy.getChild());

			// Make the physical elimination operator
			DuplicateEliminationOperator dup = new DuplicateEliminationOperator(child2);

			return dup;
		}

		// Reaching this is not possible
		return null;
		

	}

	
	
	private void addExpressions(Operator op, ArrayList<Expression> used) {
		if (op instanceof SelectOperator) {
			SelectOperator converted= (SelectOperator) op;
			ArrayList<Expression> expr = converted.getWhere();
			for(int i=0;i<expr.size();i++) {
				used.add(expr.get(i));
			}
		}
	}
	
	public void visit(LogicalPlan lp, HashMap<String, int[]> dbStatsInfo) {
		this.dbStatsInfo = dbStatsInfo;
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
			HashMap<String[], ArrayList<Expression>> conditions, ArrayList<Expression> notUsed, UnionFind uf, HashSet<Expression> usedJoin) {
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
					}
					else {
						// The problem with this condition not showing up in the query is probably due to the 
						// fact that there are other variables in the way of this problem
						if (allExpr.get(p) instanceof EqualsTo) {
							EqualsTo leftExpression = (EqualsTo) allExpr.get(p);
							Expression leftAttribute = leftExpression.getLeftExpression();
							String leftAttributeValue = leftAttribute.toString();
							UnionFindElement ufe = uf.find(leftAttributeValue);
							// Check if switching the conditions will get the conditions in our query to end up 
							// being populated with the right conditions and the right values.
							// Hashset only checks operations added in the join not the select.
							if (ufe.getMaxValue() !=ufe.getMinValue()) {
								filteredConditions.add(allExpr.get(p));
							}
						}
					}
				}
			}
		}
		
		// Check if join condition used already+ add to used
		ArrayList<Expression> finalConditions = new ArrayList<Expression>();
		for(Expression e: filteredConditions) {
			if(usedJoin.contains(e)) {
				continue;
			}
			else {
				usedJoin.add(e);
				finalConditions.add(e);
			}
		}
//		System.out.println(joinCondition);
//		System.out.println(filteredConditions);
		return finalConditions;
	}
	
	private HashMap<String[],ArrayList<Expression>> updateConditions(HashMap<String[],ArrayList<Expression>> prevConditions, ArrayList<Expression> notUsed, ArrayList<UnionFindElement> ufConstraints, ArrayList<Expression> used){
		HashMap<String[],ArrayList<Expression>> updatedConditions= new HashMap<String[],ArrayList<Expression>>();

//		System.out.println("Before the used");
//		System.out.println(used);
//		System.out.println("After the used");
		for(String[] element: prevConditions.keySet()) {
			// Get the expressions associated with the current element
			ArrayList<Expression> curr = prevConditions.get(element);
			for(Expression cond: curr) {
				if (used.contains(cond)) {
//					System.out.println("Inside of this loop overlapping conditions");
					continue;
				}
				if (notUsed.contains(cond)) {
					// Check if the the element key exists in the new HashMap
					if (updatedConditions.containsKey(element)) {// If it does contain this element then add it to the list of arraylist expression for this table
						updatedConditions.get(element).add(cond);
					}
					else {
						updatedConditions.put(element, new ArrayList<Expression>());
						updatedConditions.get(element).add(cond);
					}
				}
				else {
					// Check if an instance of equal expression otherwise do not add anything and go on to the next iteration
					if(cond instanceof EqualsTo) {
						// Cast the expression to EqualsTo
						EqualsTo converted= (EqualsTo)cond;
						// Get the left attribute value
						Expression left= converted.getLeftExpression();
						String leftAttr= left.toString();// Loop over the unionfindconstraints
						for(int i=0;i<ufConstraints.size();i++) {
							UnionFindElement ufElement= ufConstraints.get(i);
							if (ufElement.getAttributeSet().contains(leftAttr)) {
								// Get the min element and get the max element.
								// If they are equal then this conditon is fine since that means some
								// conditions made them equal and the conditon will be handled by select operators
								// underneath the join operators.
								if(!(ufElement.getMaxValue()==ufElement.getMinValue())) {
									if(updatedConditions.containsKey(element)) {
										updatedConditions.get(element).add(cond);
									}
									else {
										updatedConditions.put(element, new ArrayList<Expression>());
										updatedConditions.get(element).add(cond);
									}
								}
							}
						}
					}
				}

			}
		}

		return updatedConditions;
	}
	
	private void updateUsedJoinExpressions(ArrayList<Expression> used,HashSet<Expression> usedJoinExpression) {
		for(int i=0;i<used.size();i++) {
			if(usedJoinExpression.contains(used.get(i))) {
				continue;
			}
			else {
				usedJoinExpression.add(used.get(i));
			}
		}
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