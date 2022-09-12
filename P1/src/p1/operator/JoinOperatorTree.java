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
		allTables.add((Join)from);
		for(int i=0;i<joins.size();i++){
			allTables.add((Join)joins.get(i));
		}
		
		/*create the tree */
		JoinOperatorNode left = null;
		for (Join table : allTables) {
			// make the expression to create JoinOperatorNode
			JoinOperatorNode node = new JoinOperatorNode(table.toString(),null,null,null);
			if (left == null) {
				left = node;
			}
			else {
				JoinOperatorNode parentNode = null;
				parentNode.setLeftChild(left);
				parentNode.setRightChild(node);
			}
		}
		root = left;
	}
}
