package p1.unionfind;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
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

public class UnionFindVisitor implements ExpressionVisitor{
	
	private Expression expr;
	private UnionFind unionGrouping;
	private ArrayList<Expression> notUsable;
	
	public UnionFindVisitor(Expression expr) {
		this.expr=expr;
		this.notUsable=new ArrayList<Expression>();
		this.unionGrouping= new UnionFind();
	}
	
	/**
	 * Retrieves the UnionFind grouping for inputted expression.
	 * @return UnionFind containing all of the UnionFindElements.
	 */
	public UnionFind getUnionFind() {
		return this.unionGrouping;
	}
	
	/**
	 * Retrieves all of the expression that are not usable in the union find.
	 * @return ArrayList<Expression> containing all of the expressions that cannot be used 
	 * in the union find.
	 */
	public ArrayList<Expression> getnotUsableExpression(){
		return notUsable;
	}
	
	@Override
	public void visit(AndExpression arg0) {
		// Get the left and the right expression. The left expression can potentially be more and expressions
		Expression left= arg0.getLeftExpression();
		Expression right= arg0.getRightExpression();
		
		UnionFindVisitor leftUnionVisitor= new UnionFindVisitor(left);
		UnionFindVisitor rightUnionVisitor= new UnionFindVisitor(right);
		left.accept(leftUnionVisitor);
		right.accept(rightUnionVisitor);
		
		// The leftUnionVisitor will make the unionvisitor for the left expression
		// The rightUnionVisitor will make the unionVisitor for the right expression
//		System.out.println("Entered in here for the and expression");
		UnionFind leftUnionFind= leftUnionVisitor.getUnionFind();
//		System.out.println(leftUnionFind);
//		System.out.println("after that part");
		UnionFind rightunionFind= rightUnionVisitor.getUnionFind();
		ArrayList<Expression> leftNotUsable= leftUnionVisitor.getnotUsableExpression();
		ArrayList<Expression> rightNotUsable= rightUnionVisitor.getnotUsableExpression();
		
		ArrayList<Expression> allNotUsable= new ArrayList<Expression>();
		allNotUsable.addAll(leftNotUsable);
		allNotUsable.addAll(rightNotUsable);
		
		UnionFind finalUnion= UnionFind.mergeUnions(leftUnionFind, rightunionFind);
//		System.out.println(leftUnionFind);
//		System.out.println(rightunionFind);
//		System.out.println("An error happened in here somehow there is only one element in the final result");
		// Set the final unionGrouping to be equal to the finalGrouping that is outputted to us
		// as a result.
		this.unionGrouping=finalUnion;
		this.notUsable=allNotUsable;
	}
	
	
	
	@Override
	public void visit(EqualsTo arg0) {
		// All comparisons the operator either contain one element or they contain two elements
//		System.out.println("we entered the equals part");
		Expression left= arg0.getLeftExpression();
		Expression right=arg0.getRightExpression();
		UnionFind ret = new UnionFind();
		// The left attribute will always have an attribute so we do not need to check that element.
		String leftValue= left.toString();
		String rightValue= right.toString();
		Integer possibleValue=null;
		if(rightValue.matches("[0-9]+")) {
			possibleValue=Integer.parseInt(rightValue);
		}
		
		// We should not have to anything with aliases when we are adding these expression together
		if(possibleValue==null) {
			UnionFindElement leftAttribute= ret.find(leftValue);
			UnionFindElement rightAttribute=ret.find(rightValue);
			UnionFindElement combined = ret.union(leftAttribute, rightAttribute);
			combined.setEquality(true);
			ret.getUnionElement().add(combined);
//			System.out.println("Entered in here");
//			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
//			System.out.println(combined);
			unionGrouping=ret;
		}
		else {
			// Equality constraint means that both the lower and the upper bound value will
			// be the same for the problem.
			UnionFindElement leftAttribute=ret.find(leftValue);
			leftAttribute.setMaxValue(possibleValue);
			leftAttribute.setMinValue(possibleValue);
			leftAttribute.setEquality(true);
			ret.getUnionElement().add(leftAttribute);
			unionGrouping=ret;
		}
		
		
		
	}

	@Override
	public void visit(GreaterThan arg0) {
		
		Expression left= arg0.getLeftExpression();
		Expression right= arg0.getRightExpression();
		UnionFind ret= new UnionFind();
		// Left expression always holds an attribute guaranteed by the document
		String leftValue= left.toString();
		String rightValue= right.toString();
		Integer possibleValue=null;
		if(rightValue.matches("[0-9]+")) {
			possibleValue=Integer.parseInt(rightValue);
		}
		
		// If both of the references on both side refer to attributes then it is not usable.
		if(possibleValue==null) {
			notUsable.add(arg0);
		}
		else {
			UnionFindElement leftAttribute= ret.find(leftValue);
			// Greater than so we do not want to include curr value in the lower bound
			// we want to get the lower bound value plus one since the smallest value in the lower
			// bound is included.
			leftAttribute.setMinValue(possibleValue+1);
			ret.getUnionElement().add(leftAttribute);
			unionGrouping=ret;
		}
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		Expression left= arg0.getLeftExpression();
		Expression right= arg0.getRightExpression();
		UnionFind ret= new UnionFind();
		
		String leftValue=left.toString();
		String rightValue=right.toString();
		Integer possibleValue=null;
		if(rightValue.matches("[0-9]+")) {
			possibleValue=Integer.parseInt(rightValue);
		}
		
		// If both of the values are the attribute values then that is an invalid operation
		// to our union find
		if (possibleValue==null) {
			notUsable.add(arg0);
		}
		else {
			UnionFindElement leftAttribute= ret.find(leftValue);
			leftAttribute.setMinValue(possibleValue);
			ret.getUnionElement().add(leftAttribute);
			unionGrouping=ret;
		}
		
	}
	
	@Override
	public void visit(MinorThan arg0) {
		Expression left= arg0.getLeftExpression();
		Expression right=arg0.getRightExpression();
		UnionFind ret= new UnionFind();
		
		String leftValue=left.toString();
		String rightValue=right.toString();
		Integer possibleValue=null;
		
		if(rightValue.matches("[0-9]+")) {
			possibleValue=Integer.parseInt(rightValue);
		}
		
		// If both sides of the expression are attribute then that means that
		// this is not usable in the unionfind
		if (possibleValue==null) {
			notUsable.add(arg0);
		}
		else {
			UnionFindElement leftAttribute= ret.find(leftValue);
			leftAttribute.setMaxValue(possibleValue-1);
			ret.getUnionElement().add(leftAttribute);
			unionGrouping=ret;
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right= arg0.getRightExpression();
		UnionFind ret= new UnionFind();
		
		String leftValue=left.toString();
		String rightValue=right.toString();
		Integer possibleValue=null;
		
		if(rightValue.matches("[0-9]+")) {
			possibleValue=Integer.parseInt(rightValue);
		}
		
		// If both side are attributes then this is not usable in our union find.
		// just from the specification in the project.
		if (possibleValue==null) {
			notUsable.add(arg0);
		}
		else {
			UnionFindElement leftAttribute= ret.find(leftValue);
			leftAttribute.setMaxValue(possibleValue);
			ret.getUnionElement().add(leftAttribute);
			unionGrouping=ret;
		}
		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// The notequals is not usable in the union find method so we just need to add
		// it to the list of elements that we cannnot use in the union find instead
		// we can use it later to allocate to the join as we see fit and dtermine where
		// we should put those conditions
		
	}

	//	THE REST OF THE METHODS UNDER HERE ARE USELESS FOR THE PROJECT.
	
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
	
	// Input for expressionVisitor
	// 1.The expression to parse.
	// 2. 
}
