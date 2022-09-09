package p1.operator;

import p1.Tuple;
import p1.QueryPlan2;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class SortOperator extends Operator {
	
	private Operator child;
	
	public SortOperator(PlainSelect ps, String fromTable) {
		child = QueryPlan2.getOperator();
		
	}

	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dump() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dump(String outputFile) {
		// TODO Auto-generated method stub
		
	} 
}

