package p1.util;

import java.util.ArrayList;
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
	
	private LogicalTree plan;
	
	public LogicalPlan(Statement query) {
		
		PlainSelect plainSelect= (PlainSelect) query;
		LogicalTree tree = new LogicalTree();
		LogicalNode root=tree.buildTree(plainSelect);
		tree.setRoot(root);
		
		plan=tree;
	}
	
	public ArrayList<LogicalNode> getOperators(LogicalNode root){
		
		if (root.leftChild()==null && root.rightChild()==null) {
			ArrayList<LogicalNode> ret = new ArrayList<LogicalNode>();
			ret.add(root);
			return ret;
		}
		
		ArrayList<LogicalNode> ret=null;
		if (root.leftChild()!=null) {
			ret=getOperators(root.leftChild());
		}
		
		ret.add(root);
		
		return ret;
		
	}
}
