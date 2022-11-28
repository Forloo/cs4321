package p1.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;

import p1.io.BPTreeReader;

/**
 * A singleton class that stores the file location of tables and their column
 * names.
 */
public class DatabaseCatalog {

	// private field that refers to the object
	private static DatabaseCatalog catalogObject;
	private HashMap<String, String> tableNames;
	private HashMap<String, ArrayList<String>> schema;
	// The temp directory to store sort files.
	private String tempDir;
	// for index information
	private HashMap<String, String[]> indexInfo;
	// index file locations
	private String indexDir;
	// table stats made by StatGen
	// key: if just table name then num tuples is the value in a one-element array.
	// If key is table name + column name, then value is 2-element array with arr[0]
	// = low, arr[1] = high. example: {Sailors: [10000], Sailors.A: [0, 10000],
	// Sailors.B: [0, 100], Boats: [1000], Boats.D: ...}
	public HashMap<String, int[]> statsInfo;
	ArrayList<Integer> leaves = new ArrayList<Integer>();		

	//
	/*
	 * Constructor for a DatabaseCatalog: An object that gives us access to tables
	 * and their schemas
	 */
	private DatabaseCatalog(File[] fileList, File schemaFile, String tempDir, File indexInfo, String indexDir) {

		tableNames = new HashMap<String, String>();
		schema = new HashMap<String, ArrayList<String>>();
		this.tempDir = tempDir;
		this.indexInfo = new HashMap<String, String[]>();
		this.indexDir = indexDir;
		statsInfo = new HashMap<String, int[]>();

		// add index file names to use (key is the file name + "." + column name, first
		// list is the clustered variable, second for order
		try {
			Scanner fileReader1 = new Scanner(indexInfo);
			String nextL = fileReader1.nextLine();
			while (nextL != null) {
				String[] splitStr = nextL.split("\\s+"); // split by spaces
				this.indexInfo.put(splitStr[0] + "." + splitStr[1], Arrays.copyOfRange(splitStr, 2, splitStr.length));
				nextL = fileReader1.nextLine();
			}
			fileReader1.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchElementException e2) { // thrown by calling nexLine

		}
		
		// get number of leaves in each index for section 3.3 
		HashMap<String, String[]> info = getIndexInfo();
		Set<String> keys = info.keySet();
		Iterator<String> itr = keys.iterator();					

		for (int i = 0; i < keys.size(); i++) {
			while(itr.hasNext()) {
				BPTreeReader reader = new BPTreeReader(indexDir + itr.next());
				leaves.add(reader.getHeaderInfo().get(1)); //num leaves in each index 
			} 
		} 
		
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
					schema.get(tableName).add(tableName + "." + tableDef[k]);
				}
			}
		}
	}
	/**
	 * Return the index info: table name and attribute for naming index files
	 * 
	 * @return list of table name concatenated by a period followed by the attribute
	 *         for index files
	 */
	public HashMap<String, String[]> getIndexInfo() {
		return this.indexInfo;
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
	 * Return info about number of leaves in each index 
	 * 
	 * @return list of integers representing the number of leaves in each index 
	 */
	public ArrayList<Integer> getNumLeaves() {
		return this.leaves;
	}

	/**
	 * Initialize a DatabaseCatalog object
	 *
	 * @param fileList: A list of files specifying table information
	 * @param schema:   A file specifying the structure of tables.
	 * @return A DatabaseCatalog object
	 */
	public static DatabaseCatalog getInstance(File[] fileList, File schema, String tempDir, File indexInfo,
			String indexDir) {
		// write code that allows us to create only one object
		// access the object as per our need
		if (catalogObject == null) {
			catalogObject = new DatabaseCatalog(fileList, schema, tempDir, indexInfo, indexDir);
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

	/**
	 * Gets the temp directory for external sort.
	 */
	public String getTempDir() {
		return tempDir;
	}

	/**
	 * Gets the path of the folder with indexes.
	 * 
	 * @return the path of the index directory
	 */
	public String getIndexDir() {
		return indexDir;
	}

}