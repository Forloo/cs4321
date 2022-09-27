package p1.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
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
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * A class that parses expressions and stores what tables are used in each
 * expression.
 */
public class ExpressionParser implements ExpressionVisitor {

	// Keeps track of all the expression
	private ArrayList<Expression> allExpr;
	// A temp field so that we can call accept commands
	private Expression expr;
	// String array key tells us what tables are needed. Expression list
	// is the list of expression associated with these two tables.
	private HashMap<String[], ArrayList<Expression>> tablesNeeded;

	/**
	 * Constructor for expressionParser class
	 *
	 * @param expr The expression to break down.
	 */
	public ExpressionParser(Expression expr) {
		this.expr = expr;
		allExpr = new ArrayList<Expression>();
		tablesNeeded = new HashMap<String[], ArrayList<Expression>>();
	}

	/**
	 * Retrieves the list of expressions
	 *
	 * @return A list of expressions
	 */
	public ArrayList<Expression> getList() {
		return allExpr;
	}

	/**
	 * A recursive method to get all subexpressions.
	 *
	 * @param arg0 The and statement to break into smaller pieces.
	 * @return A list of expressions
	 */
	private ArrayList<Expression> getAllExpr(AndExpression arg0) {

		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		ExpressionParser leftParser = new ExpressionParser(left);
		ExpressionParser rightParser = new ExpressionParser(right);
		left.accept(leftParser);
		right.accept(rightParser);
		ArrayList<Expression> leftValue = leftParser.getList();
		for (int i = 0; i < leftValue.size(); i++) {
			allExpr.add(leftValue.get(i));
		}

		HashMap<String[], ArrayList<Expression>> leftTables = leftParser.getTablesNeeded();

		for (String[] key : leftTables.keySet()) {

			// Loop through our current table. Check if there are any overlapping keys
			boolean found = false;
			for (String[] key2 : this.getTablesNeeded().keySet()) {
				if (Arrays.equals(key, key2)) {
					found = true;
					for (int i = 0; i < leftTables.get(key).size(); i++) {
						this.getTablesNeeded().get(key2).add(leftTables.get(key).get(i));
					}
				}
			}
			if (!found) {
				this.getTablesNeeded().put(key, leftTables.get(key));
			}
		}
		ArrayList<Expression> rightValue = rightParser.getList();
		for (int j = 0; j < rightValue.size(); j++) {
			allExpr.add(rightValue.get(j));
		}

		HashMap<String[], ArrayList<Expression>> rightTables = rightParser.getTablesNeeded();

		for (String[] key : rightTables.keySet()) {
			// Loop through our current table. Check if there any overlapping keys
			boolean found = false;
			for (String[] key2 : this.getTablesNeeded().keySet()) {
				if (Arrays.equals(key, key2)) {
					found = true;
					for (int i = 0; i < rightTables.get(key).size(); i++) {
						this.getTablesNeeded().get(key2).add(rightTables.get(key).get(i));
					}
				}
			}
			if (!found) {
				this.getTablesNeeded().put(key, rightTables.get(key));
			}
		}

		return allExpr;
	}

	/**
	 * A method to retrieve the list of tables and their associated expressions.
	 *
	 * @return
	 */
	public HashMap<String[], ArrayList<Expression>> getTablesNeeded() {
		return tablesNeeded;
	}

	/**
	 * Retrieves the table needed for a binaryexpressions
	 *
	 * @param arg0 The binary expression
	 * @return The table or tables needed to computer the binary expression.
	 */
	private String[] getTables(BinaryExpression arg0) {
		String tblNeed = "";
		if (arg0.getLeftExpression().toString().contains(".")) {
			tblNeed = arg0.getLeftExpression().toString().substring(0,
					arg0.getLeftExpression().toString().indexOf("."));
		}
		if (arg0.getRightExpression().toString().contains(".")) {
			tblNeed = tblNeed + "," + arg0.getRightExpression().toString().substring(0,
					arg0.getRightExpression().toString().indexOf("."));
		}
		String[] arr = tblNeed.split(",");
		Arrays.sort(arr);
		return arr;

	}
	/**
	 * Recursivley call the and expression to get smaller expressions
	 */
	@Override
	public void visit(AndExpression arg0) {
		ArrayList<Expression> ret = this.getAllExpr(arg0);
		allExpr = ret;
	}

	/**
	 * One of the base cases: Add this expression to our list of expressions.
	 */
	@Override
	public void visit(EqualsTo arg0) {
		String[] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);

	}

	/**
	 * One of the base cases: Add this expression to our list of expressions.
	 */
	@Override
	public void visit(GreaterThan arg0) {
		String[] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);

	}

	/**
	 * One of the base cases: Add this expression to our list of expressions.
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		String[] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);

	}

	/**
	 * One of the base cases. Add this case to our list of base cases.
	 */
	@Override
	public void visit(MinorThan arg0) {
		String[] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);

	}

	/**
	 * One of the base cases. Add this case to our list of bases cases.
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		String[] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);
	}

	/**
	 * One of the base cases. Add this case to our list of base cases.
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		String[] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);

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
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Between arg0) {
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


}
