package p1.logicaloperator;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalScan extends LogicalOperator{

	private String tableName;
	private PlainSelect plainSelect;
	
	public LogicalScan(PlainSelect plainselect,String fromTable) {
		this.plainSelect=plainSelect;
		this.tableName=fromTable;
	}
	
	
	public PlainSelect getInfo() {
		return plainSelect;
	}
	
	public String getName() {
		return tableName;
	}
	
}
