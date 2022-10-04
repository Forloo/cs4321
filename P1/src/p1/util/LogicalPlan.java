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
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalJoin;
import p1.logicaloperator.LogicalOperator;
import p1.logicaloperator.LogicalProject;
import p1.logicaloperator.LogicalScan;
import p1.logicaloperator.LogicalSort;
import p1.logicaloperator.LogicalUnique;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.JoinOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;

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
		HashMap<String[], ArrayList<Expression>> expressionInfoAliases= null;
		HashMap<String,ArrayList<Expression>> expressionInfo=null;
		if (where!=null) {
			ExpressionParser parse= new ExpressionParser(where);
			where.accept(parse);
			expressionInfoAliases= parse.getTablesNeeded();
			expressionInfo=parse.getTablesNeededString();
		}
		
		// Extract aliases
		Aliases.getInstance(plainSelect);
		String fromTable = from.toString();
		if (from.getAlias() != null) {
			fromTable = Aliases.getTable(from.getAlias());
		}
		
		LogicalOperator child =null;
		boolean joinUsed=false;
		
		// Check if there is more than one table being used 
		if(joins!=null) {
			boolean fromUsed=false;
			LogicalJoin prev=null;
			// Iterate through the join tables and make the logical join
			for(int i=0;i<joins.size();i++) {
				if(!fromUsed) {
					// there is no where condition meaning that we can just make both of
					// the operators just scan tables
					if(expressionInfo==null) {
						LogicalScan first= new LogicalScan(from.toString());
						LogicalScan second= new LogicalScan(joins.get(i).toString());
						String combinedName = from.toString()+","+joins.get(i).toString();
						LogicalJoin temp= new LogicalJoin(combinedName,first,second,null);
						prev=temp;
					}
					else {
						// Get the where conditions for all of the tables and assign them accordingly
						LogicalOperator first= null;
						if(expressionInfo.containsKey(from.toString())) {
							ArrayList<Expression> conditions= expressionInfo.get(from.toString());
							LogicalScan scanone= new LogicalScan(from.toString());
							LogicalFilter selectone= new LogicalFilter(scanone,conditions.get(0));
							first=selectone;
						}
						else {
							LogicalScan scanone = new LogicalScan(from.toString());
							first=scanone;
						}
						
						// Do the same for the join table
						LogicalOperator second =null;
						if(expressionInfo.containsKey(joins.get(i).toString())) {
							// Get the arraylist of conditions
							ArrayList<Expression> conditions2= expressionInfo.get(joins.get(i));
							LogicalScan scantwo= new LogicalScan(joins.get(i).toString());
							LogicalFilter selecttwo= new LogicalFilter(scantwo,conditions2.get(0));
							second=selecttwo;
						}
						else {
							LogicalScan scantwo= new LogicalScan(joins.get(0).toString());
							second=scantwo;
						}
						
						String combinedName=from.toString()+","+joins.get(i).toString();
						String[] tablesNeeded= combinedName.split(",");
						Arrays.sort(tablesNeeded);
						String sortedTablesNeeded= String.join(",",tablesNeeded);
						
						// Check if there is match for this table. If there is a match
						// then assign the conditions in that arraylist to the join
						ArrayList<Expression> joinCondition=null;
						if(expressionInfo.containsKey(sortedTablesNeeded)) {
							ArrayList<Expression> joinConditions= expressionInfo.get(sortedTablesNeeded);
							joinCondition=joinConditions;
						}
						
						LogicalJoin temp = new LogicalJoin(combinedName,first,second,joinCondition);
						prev=temp;
					}
					fromUsed=true;
				}
				else {
					// The from table was used meaning that the prev node is not null
					// so the left child will be a join operator node.
					if(expressionInfo==null) {
						LogicalScan first= new LogicalScan(joins.get(i).toString());
						String combinedName= prev.getTables()+","+joins.get(i).toString();
						LogicalJoin temp= new LogicalJoin(combinedName,prev,first,null);
						prev=temp;
					}
					else {
						LogicalOperator first=null;
						// Check if we should put any conditions on the right table
						if (expressionInfo.containsKey(joins.get(i).toString())) {
							ArrayList<Expression> conditions = expressionInfo.get(joins.get(i).toString());
							LogicalScan scanone= new LogicalScan(joins.get(i).toString());
							LogicalFilter selectone= new LogicalFilter(scanone,conditions.get(0));
							first=selectone;
						}
						else {
							// There are no conditions for the right table so the right is just a scan
							LogicalScan scanone= new LogicalScan(joins.get(i).toString());
							first=scanone;
						}
						
						String combinedName= prev.getTables()+","+joins.get(i).toString();
						ArrayList<Expression> joinCondition=new ArrayList<Expression>();
						String[] splitted= combinedName.split(",");
						HashSet<String> tblsNeed= new HashSet<String>();
						for(int k=0;k<splitted.length;k++) {
							tblsNeed.add(splitted[k]);
						}
						
						
						for(String[] key: expressionInfoAliases.keySet()) {
							boolean allIncluded=true;
							for(int l=0;l<key.length;l++) {
								allIncluded=allIncluded && tblsNeed.contains(key[l]);
							}
							
							if(allIncluded) {
								ArrayList<Expression> allExpr= expressionInfoAliases.get(key);
								for (int p=0;p<allExpr.size();p++) {
									joinCondition.add(allExpr.get(p));
								}
							}
						}
						
						LogicalJoin temp= new LogicalJoin(combinedName,prev,first,joinCondition);
						prev=temp;
						
					}
					
				}
			}
			child=prev;
			joinUsed=true;
		}
		
		if(!joinUsed) {
			// We need to make the scan table operator anyway 
			LogicalScan logicalscan = new LogicalScan(from.toString());
			
			// Check if there is a where clause 
			if (where!=null) {
				LogicalFilter filter = new LogicalFilter(logicalscan,where);
				child=filter;
			}
			else {
				child=logicalscan;
			}
			
		}
		
		// Check if there is projection node that is needed here 
		if(!(allColumns.get(0) instanceof AllColumns)) {
			LogicalProject project= new LogicalProject(child,allColumns);
			child=project;
		}
		
		// Check if there is a distinct
		if (distinct!=null) {
			// For distinct there is always a order by element
			LogicalSort sort = new LogicalSort(child,groupByElements);
			LogicalUnique dup = new LogicalUnique(sort);
			child=dup;
		}
		else if (groupByElements!=null) {
			LogicalSort sort= new LogicalSort(child,groupByElements);
			child=sort;
		}
		
		this.rootOperator=child;
		
	}
	
	/**
	 * Retrieves the rootOperator
	 * @return A logicalOperator determining the root of the current node.
	 */
	public LogicalOperator getOperator() {
		return rootOperator;
	}
	
	
	/**
	 * Accepts a physicalplanbuilder object and then uses it to build the physical tree.
	 * @param pb
	 */
	public void accept(PhysicalPlanBuilder pb) {
		pb.visit(this);
	}
}
