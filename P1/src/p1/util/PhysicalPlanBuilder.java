package p1.util;

import java.io.IOException;

import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.logicaloperator.*;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.JoinOperator;
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;

/**
 * Walks through logical plan and builds a physical plan
 */
public class PhysicalPlanBuilder {
	
	private Operator op;
	
	public PhysicalPlanBuilder(LogicalPlan logicalplan) {
		op = null;
	}
	
	/**
	 * Create physical duplicate elimination operator
	 */
	public void visit(LogicalUnique unique) {
		String name = unique.getName();
		PlainSelect ps = unique.getInfo();
		
		op = new DuplicateEliminationOperator(ps, name);
	}
	
	/**
	 * Create physical sort operator
	 */
	public void visit(LogicalSort sort) { 
		String name = sort.getName();
		PlainSelect ps = sort.getInfo();
		
		op = new SortOperator(ps, name);
	}
	
	/**
	 * Create physical project operator
	 */
	public void visit(LogicalProject project) {
		String name = project.getName();
		PlainSelect ps = project.getInfo();
		
		op = new ProjectOperator(ps, name);	
	}
	
	/**
	 * Create physical join operator
	 */
	public void visit(LogicalJoin join) { 
		String name = join.getName();
		PlainSelect ps = join.getInfo();
		
		op = new JoinOperator(ps, name);
	}
	
	/**
	 * Create physical scan operator
	 */
	public void visit(LogicalScan scan) throws IOException {
		String name = scan.getName();
		
		op = new ScanOperator(name);
	}
	
	/**
	 * Create physical select operator
	 */
	public void visit(LogicalFilter filter) {
		String name = filter.getName();
		PlainSelect ps = filter.getInfo();
		
		op = new SelectOperator(ps, name);
		
	}
	
}