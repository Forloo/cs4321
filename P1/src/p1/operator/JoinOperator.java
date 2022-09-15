package p1.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.ExpressionParser;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;


/**
 * An operator that processes queries on multiple files and conditions.
 */
public class JoinOperator extends Operator{
	
	private JoinOperatorTree tree;
	private ArrayList<Tuple> results;
	private int idx;
	private ArrayList<String> schema;
	private Expression where;
	
	public JoinOperator(PlainSelect plainSelect, String fromTable,DatabaseCatalog db) {
		// Split the where expression
		Expression whereClause=plainSelect.getWhere();
		ExpressionParser parse = new ExpressionParser(whereClause);
		where=plainSelect.getWhere();
		where.accept(parse);
		HashMap<String[],ArrayList<Expression>> expressionInfo= parse.getTablesNeeded();
		
		tree = new JoinOperatorTree(plainSelect,expressionInfo); 
		
		HashMap<String,ArrayList<Tuple>> tbl= tree.dfs(tree.getRoot(), db);
		for(String key: tbl.keySet()) {
			results=tbl.get(key);
			ArrayList<String> temp= new ArrayList<String>();
			String[] arr=key.split(",");
			for(int i=0;i<arr.length;i++) {
				temp.add(arr[i]);
			}
			schema=temp;
		}
		idx=0;
	}
	
	public Expression getWhere() {
		return where;
	}
	
	public ArrayList<String> getSchema(){
		return schema;
	}
	
	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	@Override
	public Tuple getNextTuple() {
		if(idx==results.size()) {
			return null;
		}
		return new Tuple(results.get(idx++).toString());
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	@Override
	public void reset() {
		idx=0;
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	@Override
	public void dump() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	@Override
	public void dump(String outputFile) {
		// TODO Auto-generated method stub
		
	}
	
	public JoinOperatorTree getRoot() {
		return tree;
	}

}
