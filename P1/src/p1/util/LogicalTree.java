package p1.util;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalJoin;
import p1.logicaloperator.LogicalProject;
import p1.logicaloperator.LogicalScan;
import p1.logicaloperator.LogicalSort;
import p1.logicaloperator.LogicalUnique;

public class LogicalTree {
	
	private LogicalNode root;
	
	public LogicalTree() {
		root=null;
	}
	
	// If there is a distinct then there is a sort operator that comes with it
	// If there is a sort then we make the sort node and then we can have the childs as whatever
	// is actually left.
	
	public LogicalNode buildTree(PlainSelect plainSelect) {
		
		// The base case will be when we either hit a scan node or when we hit a 
		// select node
		
		List allColumns = plainSelect.getSelectItems();
		
		FromItem from = plainSelect.getFromItem();
		
		Expression where=plainSelect.getWhere();
		
		Distinct distinct = plainSelect.getDistinct();
		
		List orderElement= plainSelect.getOrderByElements();
		
		List joinElement= plainSelect.getJoins();
		
		// I will make the plan assuming that for any given query only one of the joins will be used
		// and not multiple nested types of joins
		
		// Base case is either the select or scan operator
		if (where==null && ((allColumns.get(0)) instanceof AllColumns) && distinct==null && orderElement==null && joinElement==null) {
			LogicalScan op = new LogicalScan(plainSelect,from.toString());
			// Make it into a node
			LogicalNode leaf= new LogicalNode(op,null,null);
			return leaf;
		}
		
		// Other base case 
		if (((allColumns.get(0)) instanceof AllColumns) && distinct ==null && orderElement==null && joinElement==null && where!=null) {
			LogicalFilter op = new LogicalFilter(plainSelect,from.toString());
			LogicalNode leaf = new LogicalNode(op,null,null);
			return leaf;
		}
		
		// Join is a base case
		if (((allColumns.get(0)) instanceof AllColumns) && distinct ==null && orderElement==null && joinElement!=null) {
			LogicalJoin op = new LogicalJoin(plainSelect,from.toString());
			LogicalNode leaf= new LogicalNode(op,null,null);
			return leaf;
		}
		
		// If it is none of the top three then it must be either distinct, sort or a projection that we need.
		if(distinct!=null) {
			LogicalUnique op= new LogicalUnique(plainSelect,from.toString());
			LogicalNode root= new LogicalNode(op,null,null);
			// Insert the recursive call here
			
			// Trying to modify the plainSelect that we are given
			plainSelect.setDistinct(null);
			root.setLeftChild(this.buildTree(plainSelect));
			
			return root;
		}
		else if( orderElement!=null) {
			LogicalSort op= new LogicalSort(plainSelect,from.toString());
			LogicalNode root=new LogicalNode (op,null,null);
			// Insert the recursive call here
			
			// Trying to modify the plainSelect that we are given
			plainSelect.setOrderByElements(null);
			root.setLeftChild(this.buildTree(plainSelect));
			return root;
		}
		else if(!((allColumns.get(0)) instanceof AllColumns)) {
			LogicalProject op= new LogicalProject(plainSelect,from.toString());
			LogicalNode root= new LogicalNode(op,null,null);
			// Insert the recursive call here
			
			// Trying to modify the plainSelect that we are given
			AllColumns colValue= new AllColumns();
			List items= new ArrayList<Object>();
			items.add(colValue);
			plainSelect.setSelectItems(items);
			root.setLeftChild(this.buildTree(plainSelect));
			
			return root;
		}
		
		
		
		return null;
	}

}
