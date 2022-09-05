package p1.databaseCatalog;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DatabaseCatalog {
	/*
	 * NOTES: Delete notes after implementation is finished 1. The database catalog
	 * needs to keep track of the names of all tables that we have 2. For each of
	 * the objects we need to keep track of where the file for the table is located
	 * 3. Keep track of the schema for the table.
	 */

	// private field that refers to the object
	private static DatabaseCatalog catalogObject;
	private HashMap<String, String> tableNames;
	private HashMap<String, ArrayList<String>> schema;

	/*
	 * Constructor for a DatabaseCatalog: An object that gives us access to tables
	 * and their schemas
	 */
	private DatabaseCatalog(File[] fileList, File schemaFile) {

		tableNames = new HashMap<String, String>();
		schema = new HashMap<String, ArrayList<String>>();

		// Get all tables and paths
		for (int i = 0; i < fileList.length; i++) {
			File currFile = fileList[i];
			String path = currFile.getAbsolutePath();
			tableNames.put(currFile.getName(), path);
		}

		// Get all table schemas
		Path schemaPath = Paths.get(schemaFile.getAbsolutePath());
		List<String> allLines = null;
		try {
			allLines = Files.readAllLines(schemaPath);
		} catch (IOException error) {
			System.out.println("Could not find the file " + schemaFile.getName());
		}

		// Parse each table and add to our schema
		if (allLines != null) {
			for (int j = 0; j < allLines.size(); j++) {
				String[] tableDef = allLines.get(j).split(" ");
				String tableName = tableDef[0];
				schema.put(tableName, new ArrayList<String>());
				for (int k = 1; k < tableDef.length; k++) {
					schema.get(tableName).add(tableDef[k]);
				}
			}
		}
	}

	/**
	 * Return the DatabaseCatalog object
	 *
	 * @return A DatabaseCatalog object
	 */
	public static DatabaseCatalog getInstance() {
		// returns the singleton object
		return catalogObject;
	}

	/**
	 * Initialize a DatabaseCatalog object
	 *
	 * @param fileList: A list of files specifying table information
	 * @param schema:   A file specifying the structure of tables.
	 * @return A DatabaseCatalog object
	 */
	public static DatabaseCatalog getInstance(File[] fileList, File schema) {
		// write code that allows us to create only one object
		// access the object as per our need
		if (catalogObject == null) {
			catalogObject = new DatabaseCatalog(fileList, schema);
		}

		// returns the singleton object
		return catalogObject;
	}

	/**
	 * Retrieves the table names and their paths
	 *
	 * @return A Hashmap containing the names of the tables and their absolute file
	 *         path
	 */
	public HashMap<String, String> getNames() {

		return tableNames;
	}

	/**
	 * Retrieves the schema for the DatabaseCatalog object
	 *
	 * @return A HashMap containing the names of the tables and their structure.
	 */
	public HashMap<String, ArrayList<String>> getSchema() {
		return schema;
	}

}