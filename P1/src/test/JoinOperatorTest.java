package test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;
import p1.operator.JoinOperator;
import p1.operator.JoinOperatorTree;


public class JoinOperatorTest {

	String queriesFile = "input" + File.separator + "queries.sql";
	String queriesOutput = "output";
	String dataDir = "input" + File.separator + "db" + File.separator;

	// Get the file list containing all file inputs
	File inputDir = new File(dataDir + "data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	File schema = new File(dataDir + "schema.txt");
	
	@Test
	public void JoinOperatorTesting() {
		for (int i = 0; i < allFiles.length; i++) {
			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
			fileList[i] = file;
		}
		
		
		
		DatabaseCatalog.getInstance(fileList, schema);
		
		ArrayList<Statement> queries= new ArrayList<Statement>();
		
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
		}
		catch(Exception err) {
			System.out.println("The file you are looking for was not found");
	}
		
		// Get the fourth query that requires a join
		Statement fourth=queries.get(4);
		
		Select select=(Select) fourth;
		PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
		JoinOperator join = new JoinOperator(plainSelect,"",DatabaseCatalog.getInstance());
		
		// Test that the final schema table is correct 
		ArrayList<String> results= new ArrayList<String>();
		results.add("A");
		results.add("B");
		results.add("C");
		results.add("G");
		results.add("H");
		
		assertEquals(results,join.getSchema());
		
		// Test that the first five rows of the joined tuple is correct.
		assertEquals("1,200,50,1,101",join.getNextTuple().toString());
		assertEquals("1,200,50,1,102",join.getNextTuple().toString());
		assertEquals("1,200,50,1,103",join.getNextTuple().toString());
		assertEquals("1,200,50,2,101",join.getNextTuple().toString());
		assertEquals("1,200,50,3,102",join.getNextTuple().toString());
		
		// Test that the reset works
		join.reset();
		
		assertEquals("1,200,50,1,101",join.getNextTuple().toString());
		assertEquals("1,200,50,1,102",join.getNextTuple().toString());
		assertEquals("1,200,50,1,103",join.getNextTuple().toString());
		assertEquals("1,200,50,2,101",join.getNextTuple().toString());
		assertEquals("1,200,50,3,102",join.getNextTuple().toString());
		
		
		
		
		// Three node parsing works
//		Statement threeTable = (Select) queries.get(0);
//		Select select = (Select) threeTable;
//		PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
//		JoinOperator join = new JoinOperator(plainSelect,"");
//		JoinOperatorTree tree= join.getRoot();
//		tree.dfs(tree.getRoot());
		
		
	}
	
	
}
