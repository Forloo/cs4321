package p1.util;

import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.logicaloperator.LogicalOperator;

public class LogicalPlan {
	
	// Make a tree for the logical plan
	
	// Priority for specific elements
	// 1. Distinct
	// 2. Sorting operator
	// 3. Projection 
	// 4. Join
	// 5. Selection
	// 6. Scan operator
	
	private Statement query;
	
	private LogicalOperator rootOperator;
	
	public LogicalPlan(Statement query) {
		
		PlainSelect plainSelect= (PlainSelect) query;
		
		
	}
}
