package p1.util;

import java.util.ArrayList;
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

		// Extract aliases
		Aliases.getInstance(plainSelect);
		String fromTable = from.toString();
		if (from.getAlias() != null) {
			fromTable = Aliases.getTable(from.getAlias());
		}
		
		if (plainSelect.getDistinct()!=null) {
			rootOperator= new LogicalUnique(plainSelect,from.toString());
		}
		else if (plainSelect.getOrderByElements()!=null) {
			rootOperator = new LogicalSort(plainSelect,from.toString());
		}
		else if (!(allColumns.get(0) instanceof AllColumns)) {
			rootOperator = new LogicalProject(plainSelect,from.toString());
		}
		else if(plainSelect.getJoins()!=null) {
			rootOperator =new LogicalJoin(plainSelect,from.toString());
		}
		else if (!(where==null)) {
			rootOperator= new LogicalFilter(plainSelect,from.toString());
		}
		else {
			rootOperator = new LogicalScan(plainSelect,from.toString());
		}
		
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
