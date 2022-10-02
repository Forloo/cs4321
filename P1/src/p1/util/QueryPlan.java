package p1.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.JoinOperator;
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;

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
		
		List joins= plainSelect.getJoins();
		
		// Get all of the expression conditions for each of the tables. THIS DOES NOT HANDLE ALIASES.
		// NEED TO DO THE PADDING LATER FOR ALIASES TO WORK.
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
		
		
		// This will be the child so that we can pass it into our constructor later.
		Operator child =null;
		boolean joinUsed= false;
		// Ordering 
		// Join,select,scan, sort,duplicate elimination operator
		
		if(joins!=null) {
			boolean fromUsed= false;
			JoinOperator prev=null;
			
			for(int i=0;i<joins.size();i++) {
				if(!fromUsed) {
					if (expressionInfo==null) {
						ScanOperator first=new ScanOperator(from.toString());
						ScanOperator second = new ScanOperator(joins.get(i).toString());
						String combinedName= from.toString()+","+joins.get(i).toString();
						JoinOperator temp= new JoinOperator(combinedName,first,second,null);
						prev=temp;				
					}
					else {
						Operator first=null;
						if(expressionInfo.containsKey(from.toString())) {
							// Get the list of conditions and for now lets only grab the first condition on the list of expression
							ArrayList<Expression> conditions= expressionInfo.get(from.toString());
							ScanOperator scanone= new ScanOperator(from.toString());
							SelectOperator selectone= new SelectOperator(scanone,conditions.get(0));
							first=selectone;
						}
						else {
							ScanOperator scanone = new ScanOperator(from.toString());
							first=scanone;
						}
						
						Operator second=null;
						if(expressionInfo.containsKey(joins.get(i).toString())) {
							// Get the arraylist of conditions
							ArrayList<Expression> conditions2= expressionInfo.get(joins.get(i));
							ScanOperator scantwo= new ScanOperator(joins.get(i).toString());
							SelectOperator selecttwo= new SelectOperator(scantwo,conditions2.get(0));
							second=selecttwo;
						}
						else {
							ScanOperator scantwo= new ScanOperator(joins.get(0).toString());
							second=scantwo;
						}
						String combinedName=from.toString()+","+joins.get(i).toString();
						String[] tablesNeeded= combinedName.split(",");
						Arrays.sort(tablesNeeded);
						String sortedTablesNeeded= String.join(",",tablesNeeded);
						Expression joinCondition=null;
						if(expressionInfo.containsKey(sortedTablesNeeded)) {
							ArrayList<Expression> joinConditions= expressionInfo.get(sortedTablesNeeded);
							// Grab the first element for now later on we will use something else later to merge
							// all of the conditions for the give operator altogether.
							joinCondition=joinConditions.get(0);
						}
						JoinOperator temp = new JoinOperator(combinedName,first,second,joinCondition);	
						prev=temp;
					}
				}
				else {
					if(expressionInfo==null) {
						ScanOperator first= new ScanOperator(joins.get(i).toString());
						// combine this value with the prev value
						String combinedName= prev.getTables()+","+joins.get(i).toString();
						JoinOperator temp= new JoinOperator(combinedName,prev,first,null);
						prev=temp;
					}
					else {
						Operator first=null;
						if (expressionInfo.containsKey(joins.get(i).toString())) {
							ArrayList<Expression> conditions = expressionInfo.get(joins.get(i).toString());
							ScanOperator scanone= new ScanOperator(joins.get(i).toString());
							SelectOperator selectone= new SelectOperator(scanone,conditions.get(0));
							first=selectone;
						}
						else {
							ScanOperator scanone= new ScanOperator(joins.get(i).toString());
							first=scanone;
						}
						
						String combinedName= prev.getTables()+","+joins.get(i).toString();
						JoinOperator temp= new JoinOperator(combinedName,prev,first,null);
						prev=temp;
					}
				}
			}
			child=prev;
			joinUsed=true;
		}
		
		if(!joinUsed) {
			// Make the scan operator since we will always need it 
			ScanOperator scan = new ScanOperator(from.toString());
			// Then check if there is some where condition. If there is then we need to make the select
			if (where!=null) {
				SelectOperator selectop= new SelectOperator(scan,where);
				child=selectop;
			}
			else {
				child=scan;
			}
		}
		
		// Check if there is a projection if there is a projection then make that the child after we are done
		if(!(allColumns.get(0) instanceof AllColumns)) {
			ProjectOperator project= new ProjectOperator(child,allColumns);
			child=project;
		}
		
		this.rootOperator=child;
		
		

		
		// Refactoring the code so none of these operations are valid anymore.
//		if (plainSelect.getDistinct() != null) {
//			DuplicateEliminationOperator op = new DuplicateEliminationOperator(plainSelect, fromTable);
//			rootOperator = op;
//		} else if (plainSelect.getOrderByElements() != null) {
//			SortOperator op = new SortOperator(plainSelect, fromTable);
//			rootOperator = op;
//		} else if (!(allColumns.get(0) instanceof AllColumns)) {
//			ProjectOperator op = new ProjectOperator(plainSelect, fromTable);
//			rootOperator = op;
//		} else if (plainSelect.getJoins() != null) {
//			JoinOperator op = new JoinOperator(plainSelect, fromTable);
//			rootOperator = op;
//		} else if (!(where == null)) {
//			SelectOperator op = new SelectOperator(plainSelect, fromTable);
//			rootOperator = op;
//		}
//		// If all of those conditions are not true then all we need is a scan operator
//		else {
//			ScanOperator op = new ScanOperator(fromTable);
//			rootOperator = op;
//		}
	}
	
	/**
	 * Constructor for mapping the logical plan to the physical plan.
	 * @param rootOperator
	 */
	public QueryPlan(Operator rootOperator) {
		this.rootOperator=rootOperator;
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
