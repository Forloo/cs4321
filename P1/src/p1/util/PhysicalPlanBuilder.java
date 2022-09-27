package p1.util;

import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;
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
public class PhysicalPlanBuilder implements ExpressionVisitor {
	
	// The physicalplan object
	private QueryTreePlan physicalPlan;
	// The plainselect containing the query information
	private PlainSelect plainSelect;
	
	/**
	 * The constructor for the PhysicalPlanBuilder
	 * @param plainSelect
	 */
	public PhysicalPlanBuilder(PlainSelect plainSelect) {
		physicalPlan=null;
		this.plainSelect=plainSelect;
	}
	
	/**
	 * Get the query information
	 * @return A plainSelect representing the query 
	 */
	public PlainSelect getQuery() {
		return plainSelect;
	}
	
	/**
	 * Retrieves the converted logical plan 
	 * @return A querytree containing the 
	 */
	public QueryTreePlan getPlan() {
		return physicalPlan;
	}
	
	public void visit(LogicalPlan lp) {
		// Get the list of all nodes that are in the logical plan.
		ArrayList<LogicalNode> allOps= lp.getOperators(lp.getRoot());
		
		QueryNode prev= null;
		for(int i=0;i<allOps.size();i++) {
			LogicalNode curr= allOps.get(i);
			LogicalOperator opValue = curr.getLogicalOperator();
			
			if (opValue instanceof LogicalUnique) {
				DuplicateEliminationOperator dup = new DuplicateEliminationOperator(this.getQuery(),this.getQuery().getFromItem().toString());
				QueryNode node = new QueryNode(dup,null,null);
				if (prev==null) {
					prev=node;
				}
				else {
					node.setLefttChild(prev);
					prev=node;
				}
			}
			else if (opValue instanceof LogicalSort) {
				SortOperator sort = new SortOperator(this.getQuery(),this.getQuery().getFromItem().toString());
				QueryNode node= new QueryNode(sort,null,null);
				if (prev==null) {
					prev=node;
				}
				else {
					node.setLefttChild(prev);
					prev=node;
				}
			}
			else if (opValue instanceof LogicalProject) {
				ProjectOperator project = new ProjectOperator(this.getQuery(),this.getQuery().getFromItem().toString());
				QueryNode node= new QueryNode(project,null,null);
				if (prev==null) {
					prev=node;
				}
				else {
					node.setLefttChild(prev);
					prev=node;
				}
			}
			else if (opValue instanceof LogicalJoin) {
				JoinOperator join= new JoinOperator(this.getQuery(),this.getQuery().getFromItem().toString());
				QueryNode node= new QueryNode(join,null,null);
				if (prev==null) {
					prev=node;
				}
				else {
					node.setLefttChild(prev);
					prev=node;
				}
			}
			else if (opValue instanceof LogicalFilter) {
				SelectOperator select = new SelectOperator(this.getQuery(),this.getQuery().getFromItem().toString());
				QueryNode node= new QueryNode(select,null,null);
				if (prev==null) {
					prev=node;
				}
				else {
					node.setLefttChild(prev);
					prev=node;
				}
			}
			else if (opValue instanceof LogicalScan) {
				ScanOperator scan = new ScanOperator(this.getQuery().getFromItem().toString());
				QueryNode node= new QueryNode(scan,null,null);
				
				if (prev==null) {
					prev=node;
				}
				else {
					node.setLefttChild(prev);
					prev=node;
				}
			}
		}
		QueryTree converted= new QueryTree();
		converted.setRoot(prev);
		QueryTreePlan updatedPlan = new QueryTreePlan(converted);
		physicalPlan=updatedPlan;
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