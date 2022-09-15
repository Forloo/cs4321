package p1;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.databaseCatalog.*;
import p1.operator.*;
public class QueryPlan {
	
	// The root operator for the queryPlan
	private Operator rootOperator;
	
	/**
	 * Constructs a query plan object for the given query.
	 * @param query Inputed query file.
	 * @param db: Database Catalog object holding table names and their schema.
	 */
	public QueryPlan(Statement query,DatabaseCatalog db) {
		Select select = (Select) query;
		
		
		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
		
		List allColumns = plainSelect.getSelectItems();
		
		FromItem from = plainSelect.getFromItem();
		
		Expression where = plainSelect.getWhere();
		
		if(!(allColumns.get(0) instanceof AllColumns)) {
			
			ProjectOperator op = new ProjectOperator(plainSelect,from.toString());
			rootOperator=op;
		}
		else if (!(where==null)) {
			SelectOperator op= new SelectOperator(plainSelect,from.toString());
			rootOperator=op;
		}
		// If both of those conditions are not tree then all we need is a scan operator
		else {
			ScanOperator op = new ScanOperator(from.toString());
			rootOperator=op;
		}
	}
	
	/**
	 * Retrieves the root operator .
	 * @return The root operator.
	 */
	public Operator getOperator() {
		return rootOperator;
	}
	
	
}
