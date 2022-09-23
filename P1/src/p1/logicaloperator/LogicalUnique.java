package p1.logicaloperator;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class LogicalUnique extends LogicalOperator {

	private String tableName;
	private PlainSelect plainSelect;
	
	public LogicalUnique(PlainSelect plainSelect,String fromTable) {
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
