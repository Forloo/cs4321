package p1.operator;
import p1.ExpressionEvaluator;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;
import p1.operator.*;
import java.util.ArrayList;
import java.util.Arrays;
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
	public JoinOperatorTree (PlainSelect plainSelect,HashMap<String[],ArrayList<Expression>> exprAssignment) {
		
		FromItem from =plainSelect.getFromItem();
		List joins =plainSelect.getJoins();

		
		ArrayList<Join> allTables= new ArrayList<Join>();

		// Add the from table
//		allTables.add((Join)from);
		for(int i=0;i<joins.size();i++){
			allTables.add((Join)joins.get(i));
		}
		
		/*create the tree */
		String[] splitted = from.toString().split(",");
		ArrayList<Expression> conditions=null;
		Arrays.sort(splitted);
		
//		for(int j=0;j<splitted.length;j++) {
//			System.out.println(splitted[j]);
//			System.out.println(splitted[j].length());
//		}
		
		// Loop through the hashmap to see if there is a condition
		for(String[] key: exprAssignment.keySet()) {
//			for(int n=0;n<key.length;n++) {
//				System.out.println(key[n]);
//				System.out.println(key[n].length());
//			}
			// Make a copy of the key
			String[] copy = key.clone();
			Arrays.sort(copy);
//			System.out.println(Arrays.equals(splitted, copy));
			if (Arrays.equals(splitted, copy)) {
				// Assign to this node a list of conditions
				conditions=exprAssignment.get(key);
			}
		}
		
//		System.out.println("possible solution here");
//		System.out.println(conditions);
		JoinOperatorNode left = new JoinOperatorNode(from.toString(),null,null,conditions);
		for (Join table : allTables) {
			// make the expression to create JoinOperatorNode
			String[] splitted2=table.toString().split(",");
			ArrayList<Expression> conditionstwo= null;
			// Loop through the hashmap
			for(String[] key: exprAssignment.keySet()) {
				String[] copy=key.clone();
				Arrays.sort(copy);
				if (Arrays.equals(splitted2, copy)) {
					conditionstwo=exprAssignment.get(key);
				}
			}
			JoinOperatorNode node = new JoinOperatorNode(table.toString(),null,null,conditionstwo);
			if (left == null) {
				left = node;
			}
			else {
				String combinedname= left.getTableName()+ "," +node.getTableName();
				String[] splitted3= combinedname.split(",");
				ArrayList<Expression> conditionsthree= null;
				Arrays.sort(splitted3);
				for(int i=0;i<splitted3.length;i++) {
					System.out.println(splitted3[i]);
				}
//				System.out.println("endfor");
				//Loop through the hashmap
				for(String[] key: exprAssignment.keySet()) {
					String[] copy=key.clone();
					Arrays.sort(copy);
					if (Arrays.equals(splitted3,copy)) {
						conditionsthree=exprAssignment.get(key);
					}
				}
				JoinOperatorNode parentNode = new JoinOperatorNode(combinedname,left,node,conditionsthree);
				left=parentNode;
			}
		}
//		System.out.println("==================");
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
		
		//Step one: Fix the prefix situation for all of the nodes
		// Step two: Introduce the where clause first for only leaf nodes
		// Step three: Have the where clause for the non-leaf nodes too
		
		
		if(root.getLeftChild()==null && root.getRightChild()==null) {
			ArrayList<Tuple> ret = new ArrayList<Tuple>();
			Tuple temp=null;
			temp=root.getLeafHelper().getNextTuple();
			ArrayList<Expression> conditions= root.getWhere();
			
			// If there are conditions then this becomes a select
			if (conditions!=null) {
				while(temp!=null) {
					boolean allValid=true;
					for(int i=0;i<conditions.size();i++) {
						ExpressionEvaluator exprtwo = new ExpressionEvaluator(temp,db.getSchema().get(root.getTableName()));
						Expression curr= conditions.get(i);
						curr.accept(exprtwo);
						allValid=allValid && (Boolean.parseBoolean(exprtwo.getValue()));
					}
					if (allValid) {
						ret.add(temp);
						temp=root.getLeafHelper().getNextTuple();
					}
					
				}
			}
			else {
				while(temp!=null) {
					ret.add(temp);
					temp=root.getLeafHelper().getNextTuple();
				}
			}
			// Always reset the idx
			root.getLeafHelper().reset();
			// Get the string representation of this tuple
			ArrayList<String> schema= db.getSchema().get(root.getTableName());
			String str="";
			for(int i=0;i<schema.size();i++) {
				//I need to add the tablename Prefix for each of these tables otherwise we cant do the comparisons.
//				System.out.println(schema.get(i));
				if (i==schema.size()-1) {
					str=str+schema.get(i);
				}
				else {
					str=str+schema.get(i)+",";
				}
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
				String bothTuple=curr.toString()+","+curr2.toString();
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
			ArrayList<Expression> ret= root.getWhere();
			return root;
		}
		
		if (root.getLeftChild()!=null) {
			dfs(root.getLeftChild());
		}
		
		if (root.getRightChild()!=null) {
			dfs(root.getRightChild());
		}
		
		ArrayList<Expression> ret2= root.getWhere();

		
		return root;
		
	}
}
