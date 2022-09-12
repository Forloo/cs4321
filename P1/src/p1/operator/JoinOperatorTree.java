package p1.operator;
import p1.operator.*;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class JoinOperatorTree {
	private JoinOperatorNode root;
	
	public JoinOperatorTree (PlainSelect plainSelect) {
		
		FromItem from =plainSelect.getFromItem();
		List joins =plainSelect.getJoins();

		
		ArrayList<Join> allTables= new ArrayList<Join>();

		// Add the from table
//		allTables.add((Join)from);
		for(int i=0;i<joins.size();i++){
			allTables.add((Join)joins.get(i));
		}
		
		/*create the tree */
		JoinOperatorNode left = new JoinOperatorNode(from.toString(),null,null,null);
		for (Join table : allTables) {
			// make the expression to create JoinOperatorNode
			JoinOperatorNode node = new JoinOperatorNode(table.toString(),null,null,null);
			if (left == null) {
				left = node;
			}
			else {
				String combinedname= left.getTableName()+ " " +node.getTableName();
				JoinOperatorNode parentNode = new JoinOperatorNode(combinedname,left,node,null);
				left=parentNode;
			}
		}
		root = left;
	}
	
	public JoinOperatorNode getRoot() {
		return root;
	}
	
	public JoinOperatorNode dfs(JoinOperatorNode root) {
		
		if(root.getLeftChild()==null && root.getRightChild()==null) {
			System.out.println(root.getTableName());
			return root;
		}
		
		if (root.getLeftChild()!=null) {
			dfs(root.getLeftChild());
		}
		
		if (root.getRightChild()!=null) {
			dfs(root.getRightChild());
		}
		
		System.out.println(root.getTableName());
		return root;
		
	}
}
