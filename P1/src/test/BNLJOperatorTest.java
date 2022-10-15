package test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import p1.operator.BNLJOperator;
import p1.operator.Operator;
import p1.util.DatabaseCatalog;
import p1.util.QueryPlan;
import p1.util.Tuple;

public class BNLJOperatorTest {
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
	public void bnljTest() {
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
		
		Statement querySeven = queries.get(7);
		DatabaseCatalog.getInstance().setJoinMethod(1);
		DatabaseCatalog.getInstance().setJoinPages(1);
		QueryPlan planSeven= new QueryPlan(querySeven,DatabaseCatalog.getInstance());
		BNLJOperator seventhroot= (BNLJOperator)planSeven.getOperator();
		
		// BNLJ works by first iterating through all of the outer elements for each
		// of the tuples in the inner table
		assertEquals("164,90,107,164,10",seventhroot.getNextTuple().toString());
		assertEquals("75,191,192,75,179",seventhroot.getNextTuple().toString());
		assertEquals("75,30,100,75,179",seventhroot.getNextTuple().toString());
		assertEquals("145,170,1,145,88",seventhroot.getNextTuple().toString());
		assertEquals("136,26,44,136,69",seventhroot.getNextTuple().toString());
		
		// Need to test if we move on to the next block of the outer table correctly
		for(int i=0;i<1693;i++) {
			seventhroot.getNextTuple();
		}
		// The first match in the second block section should be "13,19,111,13,107"
		assertEquals("13,19,111,13,107",seventhroot.getNextTuple().toString());
		
		// Reset the bnlj operator and the output of the tuple should be the value that
		// we got before 
		
		seventhroot.reset();
		
		assertEquals("164,90,107,164,10",seventhroot.getNextTuple().toString());
		assertEquals("75,191,192,75,179",seventhroot.getNextTuple().toString());
		assertEquals("75,30,100,75,179",seventhroot.getNextTuple().toString());
		assertEquals("145,170,1,145,88",seventhroot.getNextTuple().toString());
		assertEquals("136,26,44,136,69",seventhroot.getNextTuple().toString());
		
		// Test if getting the first element in the second page still works
		for(int i=0;i<1693;i++) {
			seventhroot.getNextTuple();
		}
		
		assertEquals("13,19,111,13,107",seventhroot.getNextTuple().toString());
		
		seventhroot.reset();
		
		// Get the last tuple. Check that after we run out of the outer block
		// then the result is null since no result can be produced.
		for(int i=0;i<5019;i++) {
			seventhroot.getNextTuple();
		}
		
		assertEquals(null,seventhroot.getNextTuple());
		// The result should be null for all following next tuple get operations
		assertEquals(null,seventhroot.getNextTuple());
		
		// Test the bnlj operator on when the join condition has more than one table
		// and check if the output is right.
		
		Statement queryEight= queries.get(8);
		
		// Set the join condition to be the bnlj 
		DatabaseCatalog.getInstance().setJoinMethod(1);
		DatabaseCatalog.getInstance().setJoinPages(2);
		QueryPlan planEight = new QueryPlan(queryEight,DatabaseCatalog.getInstance());
		BNLJOperator eightRoot= (BNLJOperator)planEight.getOperator();
		
		// Get the outer block of the first loop the values in it should be the 
		// same for some of the values in the first outer block value that we 
		// tested.
		for(int i=0;i<eightRoot.getOuterBlock().size();i++) {
			System.out.println(i);
			System.out.println(eightRoot.getOuterBlock().get(i));
		}
		// The size of the outer output should be 408
		assertEquals(408,eightRoot.getTuplePerScan());
		
		// First value that is a match is 30
		assertEquals("12,93,143,12,30,30,63,101",eightRoot.getNextTuple().toString());
		assertEquals("12,18,57,12,30,30,63,101",eightRoot.getNextTuple().toString());
		assertEquals("12,80,128,12,30,30,63,101",eightRoot.getNextTuple().toString());
		assertEquals("12,27,7,12,30,30,63,101",eightRoot.getNextTuple().toString());
		
		// Next value that is a match is the value 57 and there is one value with the same value
		assertEquals("183,169,161,183,57,57,24,130",eightRoot.getNextTuple().toString());
		assertEquals("183,22,42,183,57,57,24,130",eightRoot.getNextTuple().toString());
		assertEquals("183,37,79,183,57,57,24,130",eightRoot.getNextTuple().toString());
		
		// The next value that is a match is the value 172 and 
		assertEquals("6,125,191,6,172,172,68,43",eightRoot.getNextTuple().toString());
		
		// Check if the reset method works for the nested blocked nested loop joins
		eightRoot.reset();
		
		// Check if the reset worked.
		assertEquals("12,93,143,12,30,30,63,101",eightRoot.getNextTuple().toString());
		assertEquals("12,18,57,12,30,30,63,101",eightRoot.getNextTuple().toString());
		assertEquals("12,80,128,12,30,30,63,101",eightRoot.getNextTuple().toString());
		assertEquals("12,27,7,12,30,30,63,101",eightRoot.getNextTuple().toString());
		assertEquals("183,169,161,183,57,57,24,130",eightRoot.getNextTuple().toString());
		assertEquals("183,22,42,183,57,57,24,130",eightRoot.getNextTuple().toString());
		assertEquals("183,37,79,183,57,57,24,130",eightRoot.getNextTuple().toString());
		assertEquals("6,125,191,6,172,172,68,43",eightRoot.getNextTuple().toString());

		
	}
}
