package p1.operator;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;
import p1.operator.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class JoinOperatorTree {
	private JoinOperatorNode root;
	
	/**
	 * Constructor for JoinOperatorTree
	 * @param plainSelect: Gives the information for the tree
	 */
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
	
	/**
	 * Retrieves the root node for the tree
	 * @return A JoinOpeartorNode or null
	 */
	public JoinOperatorNode getRoot() {
		return root;
	}
	
	public HashMap<String,ArrayList<Tuple>> dfs(JoinOperatorNode root,DatabaseCatalog db){
		
		if(root.getLeftChild()==null && root.getRightChild()==null) {
			ArrayList<Tuple> ret = new ArrayList<Tuple>();
			Tuple temp=null;
			temp=root.getLeafHelper().getNextTuple();
			while(temp!=null) {
				ret.add(temp);
				temp=root.getLeafHelper().getNextTuple();
			}
			// Always reset the idx
			root.getLeafHelper().reset();
			// Get the string representation of this tuple
			ArrayList<String> schema= db.getSchema().get(root.getTableName());
			String str="";
			for(int i=0;i<schema.size();i++) {
				str=str+schema.get(i)+",";
			}
			HashMap<String,ArrayList<Tuple>> tbl = new HashMap<String,ArrayList<Tuple>>();
			tbl.put(str, ret);
			return tbl;
			
		}
		
		HashMap<String,ArrayList<Tuple>> left= null;
		if(root.getLeftChild()!=null) {
			left=dfs(root.getLeftChild(),db);
		}
		HashMap<String,ArrayList<Tuple>> ret= new HashMap<String,ArrayList<Tuple>>();
		HashMap<String,ArrayList<Tuple>> right =null;
		
		if (root.getRightChild()!=null) {
			right=dfs(root.getRightChild(),db);
		}
		
		// Iterate to get the one arraylist
		ArrayList<Tuple> leftList=null;
		String leftName=null;
		for(String key: left.keySet()) {
			leftName=key;
			leftList=left.get(key);
		}
		
		// Do the same for right
		ArrayList<Tuple> rightList=null;
		String rightName=null;
		for(String key: right.keySet()) {
			rightName=key;
			rightList=right.get(key);
		}
		
		// New name
		String finalName=leftName+","+rightName;
		ArrayList finalList= new ArrayList<Tuple>();
		for(int i=0;i<leftList.size();i++) {
			Tuple curr=leftList.get(i);
			for(int j=0;j<rightList.size();j++) {
				Tuple curr2=rightList.get(j);
				// Need to merge both of the tuples into one.
				String bothTuple=curr.toString()+curr2.toString();
				Tuple element=new Tuple(bothTuple);
				finalList.add(element);
			}
		}
		
		ret.put(finalName, finalList);
		
		return ret;
		
		
		
		
	}
	
	/**
	 * Iterate through the node using a depth first search model.
	 * @param root
	 * @return
	 */
	public JoinOperatorNode dfs(JoinOperatorNode root) {
//		System.out.println(root.getTableName());
		
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
