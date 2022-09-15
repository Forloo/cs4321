package p1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

public class ExpressionParser implements ExpressionVisitor {
	
	private ArrayList<Expression> allExpr;
	private Expression expr;
	private HashMap<String[], ArrayList<Expression>> tablesNeeded;

	
	public ExpressionParser(Expression expr) {
		this.expr=expr;
		allExpr=new ArrayList<Expression>();
		tablesNeeded= new HashMap<String[],ArrayList<Expression>>();
	}
	
	public ArrayList<Expression> getList(){
		return allExpr;
	}
	
	private ArrayList<Expression> getAllExpr(AndExpression arg0) {
		
		Expression left=arg0.getLeftExpression();
		Expression right=arg0.getRightExpression();
		ExpressionParser leftParser= new ExpressionParser(left);
		ExpressionParser rightParser= new ExpressionParser(right);
		left.accept(leftParser);
		right.accept(rightParser);
		ArrayList<Expression> leftValue= leftParser.getList();
		for(int i=0;i<leftValue.size();i++) {
			allExpr.add(leftValue.get(i));
		}
		
		HashMap<String[],ArrayList<Expression>> leftTables= leftParser.getTablesNeeded();
		
//		System.out.println(leftTables);
//		System.out.println(this.getTablesNeeded());
		for(String[] key: leftTables.keySet()) {
//			System.out.println("hereeee");
			// Loop through our current table. Check if there are any overlapping keys
			boolean found=false;
			for(String[] key2:this.getTablesNeeded().keySet()) {
				if (Arrays.equals(key,key2)) {
					found=true;
					for(int i=0;i<leftTables.get(key).size();i++) {
						this.getTablesNeeded().get(key2).add(leftTables.get(key).get(i));
					}
				}
			}
			if (!found) {
				this.getTablesNeeded().put(key, leftTables.get(key));
			}
		}
		ArrayList<Expression> rightValue= rightParser.getList();
		for(int j=0;j<rightValue.size();j++) {
			allExpr.add(rightValue.get(j));
		}
		
		HashMap<String[],ArrayList<Expression>> rightTables = rightParser.getTablesNeeded();
		
//		System.out.println(rightTables);
//		System.out.println(this.getTablesNeeded());
//		System.out.println("here");
		
		for(String[] key: rightTables.keySet()) {
			// Loop through our current table. Check if there any overlapping keys
			boolean found=false;
			for(String[] key2:this.getTablesNeeded().keySet()) {
				if(Arrays.equals(key,key2)) {
					found=true;
					for(int i=0;i<rightTables.get(key).size();i++) {
						this.getTablesNeeded().get(key2).add(rightTables.get(key).get(i));
					}
				}
			}
			if(!found) {
				this.getTablesNeeded().put(key, rightTables.get(key));
			}
		}
		
		return allExpr;
	}
	
	public HashMap<String[],ArrayList<Expression>> getTablesNeeded(){
		return tablesNeeded;
	}
	
	private String[] getTables(BinaryExpression arg0) {
		String tblNeed="";
		if (arg0.getLeftExpression().toString().contains(".")) {
			tblNeed=arg0.getLeftExpression().toString().substring(0, arg0.getLeftExpression().toString().indexOf("."));
		}
		if (arg0.getRightExpression().toString().contains(".")) {
			tblNeed=tblNeed+","+arg0.getRightExpression().toString().substring(0, arg0.getRightExpression().toString().indexOf("."));	
		}
		String[] arr=tblNeed.split(",");
		Arrays.sort(arr);
		return arr;
		
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
		ArrayList<Expression> ret= this.getAllExpr(arg0);
		allExpr=ret;
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
		String [] arr =this.getTables(arg0);
//		System.out.println(arg0);
//		for(int i=0;i<arr.length;i++) {
//			System.out.println(arr[i]);
//		}
		this.getTablesNeeded().put(arr,allExpr);
		allExpr.add(arg0);
		
	}

	@Override
	public void visit(GreaterThan arg0) {
		String [] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		String [] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);
		
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
		String [] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);
		
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		String [] arr = this.getTables(arg0);
		this.getTablesNeeded().put(arr, allExpr);
		allExpr.add(arg0);
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		String [] arr = this.getTables(arg0);
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
	
}
