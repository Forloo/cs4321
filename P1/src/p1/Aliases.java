package p1;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * A singleton object that stores a map from aliases to actual table names.
 */
public class Aliases {
	// The singleton object.
	private static Aliases aliases;
	// The map of aliases and table names.
	private static HashMap<String, String> aliasMap;

	/*
	 * Constructor for an Aliases: An object that gives us access to aliases and
	 * their corresponding table names.
	 */
	private Aliases(PlainSelect ps) {
		aliasMap = new HashMap<String, String>();
		FromItem from = ps.getFromItem();
		String fromTable = from.toString().split(" ")[0];
		aliasMap.put(from.getAlias() == null ? fromTable : from.getAlias(), fromTable);
		aliasMap.put(fromTable, fromTable);
		List joins = ps.getJoins();
		if (joins != null) {
			for (Object c : ps.getJoins()) {
				String[] joinTable = c.toString().split(" ");
				String joinTableName = joinTable[0];
				String joinAlias = joinTable[joinTable.length - 1];
				aliasMap.put(joinAlias, joinTableName);
				aliasMap.put(joinTableName, joinTableName);
			}
		}
	}

	/**
	 * Return the Aliases object
	 *
	 * @return An Aliases object
	 */
	public static Aliases getInstance() {
		// returns the singleton object
		return aliases;
	}

	/**
	 * Initialize an Aliases object
	 *
	 * @param ps: The PlainSelect object for the query
	 * @return An Aliases object
	 */
	public static Aliases getInstance(PlainSelect ps) {
		// Resets the aliases with the new PlainSelect item for a new query
		aliases = new Aliases(ps);

		// returns the singleton object
		return aliases;
	}

	/**
	 * Get a table name from its alias.
	 *
	 * @param alias the alias for the table or the actual table name.
	 * @return the actual table name
	 */
	public static String getTable(String alias) {
		return aliasMap.get(alias);
	}
}
