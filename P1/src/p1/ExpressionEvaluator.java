package p1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import p1.databaseCatalog.DatabaseCatalog;

/**
 * A class that evaluates where conditions on a row.
 */
public class ExpressionEvaluator implements ExpressionVisitor {

	// The row with values to evaluate
	private Tuple row;
	// Column names
	private ArrayList<String> columns;
	// Table names
	private List<String> tables;
	// Truth value
	private String value;

	/**
	 * Constructor to evaluate an expression.
	 */
	public ExpressionEvaluator(Tuple t, ArrayList<String> schema) {
		row = t;
		columns = schema;
		tables = null;
	}

	/**
	 * Constructor to evaluate an expression with a self join.
	 */
	public ExpressionEvaluator(Tuple t, String tableNames) {
		row = t;
		columns = new ArrayList<String>();
		tables = Arrays.asList(tableNames.split(","));
		for (String name : tables) {
			columns.addAll(DatabaseCatalog.getInstance().getSchema().get(name));
		}
	}

	/**
	 * Evaluates the expression
	 *
	 * @return the value of the expression
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Evaluates the values of the left and right expression
	 *
	 * @param arg0 the expression to evaluate
	 * @return an array of length 2, where the first element is the left side and
	 *         the second element is the right side
	 */
	private String[] leftRightVals(BinaryExpression arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		ExpressionEvaluator leftEval;
		ExpressionEvaluator rightEval;
		if (tables != null) {
			leftEval = new ExpressionEvaluator(row, String.join(",", tables));
			rightEval = new ExpressionEvaluator(row, String.join(",", tables));
		} else {
			leftEval = new ExpressionEvaluator(row, columns);
			rightEval = new ExpressionEvaluator(row, columns);
		}
		left.accept(leftEval);
		right.accept(rightEval);
		String leftValue = leftEval.getValue();
		String rightValue = rightEval.getValue();
		return new String[] { leftValue, rightValue };
	}

	@Override
	public void visit(Column arg0) {
		if (tables != null) {
			String table = arg0.getTable().getName();
			int tableIdx = 0;

			for (String fullTable : Aliases.getAliasList()) {
				if (fullTable.substring((fullTable.lastIndexOf(" ") + 1)).equals(table)) {
					break;
				}
				tableIdx++;
			}

			int startIdx = 0;
			for (int i = 0; i < tableIdx; i++) {
				startIdx += DatabaseCatalog.getInstance().getSchema().get(tables.get(i)).size();
			}
			List<String> subColumns = columns.subList(startIdx, columns.size());
			value = row.getTuple().get(startIdx + subColumns.indexOf(arg0.getColumnName()));
		} else {
			int idx = columns.indexOf(arg0.getColumnName());
			value = row.getTuple().get(idx);
		}
	}

	@Override
	public void visit(LongValue arg0) {
		value = arg0.toString();
	}

	@Override
	public void visit(AndExpression arg0) {
		String[] values = leftRightVals(arg0);
		value = String.valueOf(Boolean.valueOf(values[0]) && Boolean.valueOf(values[1]));
	}

	@Override
	public void visit(EqualsTo arg0) {
		String[] values = leftRightVals(arg0);
		value = String.valueOf(Integer.parseInt(values[0]) == Integer.parseInt(values[1]));
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		String[] values = leftRightVals(arg0);
		value = String.valueOf(Integer.parseInt(values[0]) != Integer.parseInt(values[1]));
	}

	@Override
	public void visit(GreaterThan arg0) {
		String[] values = leftRightVals(arg0);
		value = String.valueOf(Integer.parseInt(values[0]) > Integer.parseInt(values[1]));
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		String[] values = leftRightVals(arg0);
		value = String.valueOf(Integer.parseInt(values[0]) >= Integer.parseInt(values[1]));
	}

	@Override
	public void visit(MinorThan arg0) {
		String[] values = leftRightVals(arg0);
		value = String.valueOf(Integer.parseInt(values[0]) < Integer.parseInt(values[1]));
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		String[] values = leftRightVals(arg0);
		value = String.valueOf(Integer.parseInt(values[0]) <= Integer.parseInt(values[1]));
	}

	// USELESS METHODS FOR THIS PROJECT

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
