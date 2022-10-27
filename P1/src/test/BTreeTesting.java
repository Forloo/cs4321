package test;

import org.junit.Test;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import p1.index.BTree;
import p1.index.BTreeLeafNode;
import p1.index.BTreeNode;
import p1.index.TupleIdentifier;
import p1.util.Aliases;
import p1.util.DatabaseCatalog;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class BTreeTesting {
	String queriesFile = "input" + File.separator + "queries.sql";
	String queriesOutput = "output";
	String dataDir = "input" + File.separator + "db" + File.separator;

	// Get the file list containing all file inputs
	File inputDir = new File(dataDir + "data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	File schema = new File(dataDir + "schema.txt");
	String tempDir = "././temp";
	File configFile = new File("././input/plan_builder_config.txt");
	
	@Test
	public void mappingTesting() {
		for (int i = 0; i < allFiles.length; i++) {
			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
			fileList[i] = file;
		}

		DatabaseCatalog.getInstance(fileList, schema, configFile, tempDir);

		ArrayList<Statement> queries = new ArrayList<Statement>();

		// Parse all of the queries and put them into the arraylist.

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader("././input/queries.sql"));
			Statement statement;
			int queryCount = 1;
			while ((statement = parser.Statement()) != null) {
				try {
					// Parse statement
					queries.add(statement);
				} catch (Exception e) {
					System.err.println("Exception occurred during query " + queryCount);
					e.printStackTrace();
				}
				queryCount++;
			}
		} catch (Exception err) {
			System.out.println("The file you are looking for was not found");
		}
		// After doing this testing make sure that it works when we are using table aliases.
		File file= new File("C:\\Users\\henry\\git\\cs4321\\P1\\expected_indexes\testing.txt");
		BTree testingOne = new BTree(3,false,0,file,DatabaseCatalog.getInstance().getNames().get("Sailors"),0);
		BTreeNode root=testingOne.constructTree();
		

	}
}
