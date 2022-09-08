package p1;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.databaseCatalog.*;
import p1.operator.*;
public class QueryPlan2 {
	
	// The root operator for the queryPlan
	private Operator rootOperator;
	
	/**
	 * Constructs a query plan object for the given query.
	 * @param query Inputed query file.
	 * @param db: Database Catalog object holding table names and their schema.
	 */
	public QueryPlan2(Statement query,DatabaseCatalog db) {
		// Get the select statement from the query
		Select select = (Select) query;
		// Convert the information to a plainSelect so we can the queries 
		// column information, the table we retrieve from, any where conditions
		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
		
		// A list of all the columns
		List allColumns = plainSelect.getSelectItems();
		// The table that we are getting our data from.
		FromItem from = plainSelect.getFromItem();
		// Information telling us the condition for the where class.
		Expression where = plainSelect.getWhere();
		
		// Our root operator node is projection if we are choosing specific 
		// columns and not the *
		if (!(allColumns.get(0) instanceof AllColumns)) {
			// There is no  projection class yet so placeholder will be projection
			SelectOperator op = new SelectOperator(plainSelect,from.toString());
			rootOperator=op;
		}
		// Check that where is not null meaning we need a select operator
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
