package p1.util;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalTree {
	
	private LogicalNode root;
	
	public LogicalTree() {
		root=null;
	}
	
	// If there is a distinct then there is a sort operator that comes with it
	// If there is a sort then we make the sort node and then we can have the childs as whatever
	// is actually left.
	
	public LogicalNode buildTree(PlainSelect plainSelect) {
		return null;
	}

}
