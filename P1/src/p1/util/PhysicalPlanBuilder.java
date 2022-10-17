package p1.util;

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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SubSelect;
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
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.SMJOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;
import p1.operator.TNLJOperator;

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
			ScanOperator scanleaf = new ScanOperator(cpy.getFromTable());
			return scanleaf;
		}

		if (rootOperator instanceof LogicalFilter) {
			// Cast the rootoperator to the logical filter
			LogicalFilter cpy = (LogicalFilter) rootOperator;

			ScanOperator child = (ScanOperator) generatePhysicalTree(cpy.getChild());
			// The child for select is always scan so we need to cast it
			SelectOperator select = new SelectOperator(child, cpy.getExpression());
			return select;
		}

		if (rootOperator instanceof LogicalProject) {
			// Cast the element to the logical project
			LogicalProject cpy = (LogicalProject) rootOperator;

			// Get the child element for this
			Operator child = generatePhysicalTree(cpy.getChild());

			ProjectOperator project = new ProjectOperator(child, cpy.getSelects());

			return project;
		}

		if (rootOperator instanceof LogicalJoin) {
			// Cast the element to the logical join
			LogicalJoin cpy = (LogicalJoin) rootOperator;

			// Get the left and right child
			Operator leftchild = generatePhysicalTree(cpy.getLeftChild());
			Operator rightchild = generatePhysicalTree(cpy.getRightChild());

			if (DatabaseCatalog.getInstance().getJoinMethod() == 0) { // Tuple nested loop join
				return new TNLJOperator(cpy.getTables(), leftchild, rightchild, cpy.getExpression());
			} else if (DatabaseCatalog.getInstance().getJoinMethod() == 1) { // Block nested loop join
				return new BNLJOperator(cpy.getTables(), leftchild, rightchild, cpy.getExpression(),DatabaseCatalog.getInstance().getJoinPages());
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
			if (DatabaseCatalog.getInstance().getSortMethod() == 0) { // in-memory sort
				sort = new SortOperator(child, cpy.getOrderBy());
			} else { // external sort
				sort = new ExternalSortOperator(child, cpy.getOrderBy(), DatabaseCatalog.getInstance().getSortPages(), DatabaseCatalog.getInstance().getTempDir(),0);
			}
			return sort;
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