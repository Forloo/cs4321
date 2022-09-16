package p1;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.databaseCatalog.DatabaseCatalog;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.JoinOperator;
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;

public class QueryPlan {

	// The root operator for the queryPlan
	private Operator rootOperator;

	/**
	 * Constructs a query plan object for the given query.
	 *
	 * @param query Inputed query file.
	 * @param db:   Database Catalog object holding table names and their schema.
	 */
	public QueryPlan(Statement query, DatabaseCatalog db) {
		Select select = (Select) query;

		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

		List allColumns = plainSelect.getSelectItems();

		FromItem from = plainSelect.getFromItem();

		Expression where = plainSelect.getWhere();

		// Extract aliases
		Aliases.getInstance(plainSelect);
		String fromTable = from.toString();
		if (from.getAlias() != null) {
			fromTable = Aliases.getTable(from.getAlias());
		}

		if (plainSelect.getDistinct() != null) {
			DuplicateEliminationOperator op = new DuplicateEliminationOperator(plainSelect, fromTable);
			rootOperator = op;
		} else if (plainSelect.getOrderByElements() != null) {
			SortOperator op = new SortOperator(plainSelect, fromTable);
			rootOperator = op;
		} else if (!(allColumns.get(0) instanceof AllColumns)) {
			ProjectOperator op = new ProjectOperator(plainSelect, fromTable);
			rootOperator = op;
		} else if (plainSelect.getJoins() != null) {
			JoinOperator op = new JoinOperator(plainSelect, fromTable);
			rootOperator = op;
		} else if (!(where == null)) {
			SelectOperator op = new SelectOperator(plainSelect, fromTable);
			rootOperator = op;
		}
		// If all of those conditions are not true then all we need is a scan operator
		else {
			ScanOperator op = new ScanOperator(fromTable);
			rootOperator = op;
		}
	}

	/**
	 * Retrieves the root operator .
	 *
	 * @return The root operator.
	 */
	public Operator getOperator() {
		return rootOperator;
	}

}
