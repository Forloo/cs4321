package p1.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;

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
	// The type of join to use.
	private int joinMethod;
	// Number of buffer pages to use for join.
	private int joinPages;
	// Type of sort to use.
	private int sortMethod;
	// Number of buffer pages to use for sort.
	private int sortPages;
	// 1 to use indexes, 0 to not use indexes.
	private int useIndex;
	//for index information
	private HashMap<String,ArrayList<Integer>> indexInfo;
	//
	/*
	 * Constructor for a DatabaseCatalog: An object that gives us access to tables
	 * and their schemas
	 */
	private DatabaseCatalog(File[] fileList, File schemaFile, File configFile, String tempDir, File indexInfo) {

		tableNames = new HashMap<String, String>();
		schema = new HashMap<String, ArrayList<String>>();
		this.tempDir = tempDir;
		
		//add index file names to use (key is the file name, first element of array list is the clustered variable, second for order
		try {
			Scanner fileReader1 = new Scanner(indexInfo);
			String nextL = fileReader1.nextLine(); 
			while ( nextL != null) {
				String[] splitStr = nextL.split("\\s+");//split by spaces
				ArrayList<Integer> clusAndOrder = new ArrayList<Integer>();
				clusAndOrder.add(Integer.parseInt(splitStr[1]));
				clusAndOrder.add(Integer.parseInt(splitStr[2]));
				this.indexInfo.put(splitStr[0] + "." + splitStr[1], clusAndOrder);
				nextL = fileReader1.nextLine();
				
			}
			fileReader1.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchElementException e2) {//thrown by calling nexLine
			
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

		// Get configuration file data and store in DatabaseCatalog
		try {
			Scanner fileReader = new Scanner(configFile);
			String[] joinConfig = fileReader.nextLine().split(" ");
			joinMethod = Integer.parseInt(joinConfig[0]);
			if (joinConfig.length > 1) {
				joinPages = Integer.parseInt(joinConfig[1]);
			}
			String[] sortConfig = fileReader.nextLine().split(" ");
			sortMethod = Integer.parseInt(sortConfig[0]);
			if (sortConfig.length > 1) {
				sortPages = Integer.parseInt(sortConfig[1]);
			}
			String indexConfig = fileReader.nextLine();
			useIndex = Integer.parseInt(indexConfig);

			fileReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred while parsing the configuration file.");
			e.printStackTrace();
		}
	}

	/**
	 * Return the index info: table name and attribute for naming index files
	 * @return list of table name concatenated by a period followed by the attribute for index files
	 */
	public HashMap<String,ArrayList<Integer>> getIndexInfo(){
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
	 * Initialize a DatabaseCatalog object
	 *
	 * @param fileList: A list of files specifying table information
	 * @param schema:   A file specifying the structure of tables.
	 * @return A DatabaseCatalog object
	 */
	public static DatabaseCatalog getInstance(File[] fileList, File schema, File configFile, String tempDir, File indexInfo) {
		// write code that allows us to create only one object
		// access the object as per our need
		if (catalogObject == null) {
			catalogObject = new DatabaseCatalog(fileList, schema, configFile, tempDir, indexInfo);
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
	 * Gets the join method.
	 */
	public int getJoinMethod() {
		return joinMethod;
	}

	/**
	 * Gets the number of buffer pages to use for BNLJ.
	 */
	public int getJoinPages() {
		return joinPages;
	}

	/**
	 * Gets the sort method.
	 */
	public int getSortMethod() {
		return sortMethod;
	}

	/**
	 * Gets the number of buffer pages to use for external sort.
	 */
	public int getSortPages() {
		return sortPages;
	}

	/**
	 * Set the join method to use
	 * 
	 * @param joinMethod an int representing which join method to use
	 */
	public void setJoinMethod(int joinMethod) {
		this.joinMethod = joinMethod;
	}

	/**
	 * Set the number of pages to use for the bnlj
	 * 
	 * @param joinPages an integer specifying the number of pages to use for bnlj
	 */
	public void setJoinPages(int joinPages) {
		this.joinPages = joinPages;
	}

	/**
	 * Set the sort method to use for SMJ
	 * 
	 * @param sortMethod an integer specifying the sort method
	 */
	public void setSortMethod(int sortMethod) {
		this.sortMethod = sortMethod;
	}

	/**
	 * Set the number of pages to use for sorting
	 * 
	 * @param sortPages an integer specifying the number of pages to use for sorting
	 */
	public void setSortPages(int sortPages) {
		this.sortPages = sortPages;
	}

	/**
	 * Returns whether or not to use indexes for scanning.
	 * 
	 * @return true for using indexes, false for not using indexes.
	 */
	public boolean useIndex() {
		return useIndex == 1 ? true : false;
	}

}