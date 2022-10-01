package p1.util;

import java.util.ArrayList;
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
		if (where!=null) {
			ExpressionParser parse= new ExpressionParser(where);
			where.accept(parse);
			expressionInfoAliases= parse.getTablesNeeded();
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
		
		// check if there is a join that has been used. If the join was used then
		// do not use the scan
		
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
