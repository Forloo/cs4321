package test;

import java.io.FileReader;
import java.util.ArrayList;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.Assert.assertTrue;


import java.io.File;
import java.io.FileNotFoundException;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.JoinOperator;
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;
import p1.util.*;

import org.junit.jupiter.api.Test;
import java.util.HashMap;

public class QueryPlanTest {
	
	String queriesFile = "input" + File.separator + "queries.sql";
	String queriesOutput = "output";
	String dataDir = "input" + File.separator + "db" + File.separator;

	// Get the file list containing all file inputs
	File inputDir = new File(dataDir + "data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	File schema = new File(dataDir + "schema.txt");
	
	@Test
	public void queryTesting() {
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
		
		Statement queryZero = queries.get(0);
		QueryPlan first= new QueryPlan(queryZero,DatabaseCatalog.getInstance());
		
		// Check if the refactored scan is working.
		assertTrue(first.getOperator() instanceof ScanOperator);
		Operator scanTesting= first.getOperator();
		
		assertEquals(scanTesting.getNextTuple().toString(),"64,113,139");
		assertEquals(scanTesting.getNextTuple().toString(),"181,128,129");
		assertEquals(scanTesting.getNextTuple().toString(),"147,45,118");
		assertEquals(scanTesting.getNextTuple().toString(),"81,1,195");
		assertEquals(scanTesting.getNextTuple().toString(),"75,191,192");
		
		// Check if the reset method works by confirming that the first tuple is now the first tuple in the table
		scanTesting.reset();
		assertEquals(scanTesting.getNextTuple().toString(),"64,113,139");
		
		// Testing if the refactored select is working
		Statement queryFour = queries.get(4);
		QueryPlan fourth= new QueryPlan(queryFour,DatabaseCatalog.getInstance());
		
		Operator selectTesting= fourth.getOperator();
		assertTrue(selectTesting instanceof SelectOperator);
		
		assertEquals(selectTesting.getNextTuple().toString(),"133,197,18");
		assertEquals(selectTesting.getNextTuple().toString(),"157,148,100");
		assertEquals(selectTesting.getNextTuple().toString(),"118,72,25");
		assertEquals(selectTesting.getNextTuple().toString(),"5,119,15");
		assertEquals(selectTesting.getNextTuple().toString(),"173,194,157");
		
		// Check if the rest method for this works
		selectTesting.reset();
		
		assertEquals(selectTesting.getNextTuple().toString(),"133,197,18");
		
		// Test if the project works with only a scan child
		Statement queryOne = queries.get(1);
		QueryPlan projectionOne = new QueryPlan(queryOne,DatabaseCatalog.getInstance());
		
		// Check if the root operator is the project 
		Operator projectionTestingOne = projectionOne.getOperator();
		assertTrue( projectionTestingOne instanceof ProjectOperator);
		
		assertEquals(projectionTestingOne.getNextTuple().toString(),"64");
		assertEquals(projectionTestingOne.getNextTuple().toString(),"181");
		assertEquals(projectionTestingOne.getNextTuple().toString(),"147");
		assertEquals(projectionTestingOne.getNextTuple().toString(),"81");
		assertEquals(projectionTestingOne.getNextTuple().toString(),"75");
		
		// Test to see if the reset method for the project works after refactoring
		projectionTestingOne.reset();
		
		assertEquals(projectionTestingOne.getNextTuple().toString(),"64");
		
		// Test to see if the project works when it has the select as a child instead of the scan
		Statement queryFive =queries.get(5);
		QueryPlan projectionTwo = new QueryPlan(queryFive,DatabaseCatalog.getInstance());
		Operator projectionTestingTwo= projectionTwo.getOperator();
		assertTrue( projectionTestingTwo instanceof ProjectOperator);
				
		assertEquals(projectionTestingTwo.getNextTuple().toString(),"133");
		assertEquals(projectionTestingTwo.getNextTuple().toString(),"157");
		assertEquals(projectionTestingTwo.getNextTuple().toString(),"118");
		assertEquals(projectionTestingTwo.getNextTuple().toString(),"5");
		assertEquals(projectionTestingTwo.getNextTuple().toString(),"173");
		
		// Test to see if the reset method works after re factoring
		projectionTestingTwo.reset();
		assertEquals(projectionTestingTwo.getNextTuple().toString(),"133");
		
		
		// Join Testing 
		Statement querySeven = queries.get(7);
		QueryPlan join = new QueryPlan(querySeven,DatabaseCatalog.getInstance());
		Operator joinTesting= join.getOperator();
		assertTrue(joinTesting instanceof JoinOperator);
		
		assertEquals(joinTesting.getNextTuple().toString(),"64,113,139,64,156");
		assertEquals(joinTesting.getNextTuple().toString(),"64,113,139,64,70");
		assertEquals(joinTesting.getNextTuple().toString(),"64,113,139,64,170");
		assertEquals(joinTesting.getNextTuple().toString(),"64,113,139,64,16");
		assertEquals(joinTesting.getNextTuple().toString(),"64,113,139,64,70");

		// Test if the reset works
		joinTesting.reset();
		
		assertEquals(joinTesting.getNextTuple().toString(),"64,113,139,64,156");
		assertEquals(joinTesting.getNextTuple().toString(),"64,113,139,64,70");
		
//		System.out.println("delimitered");
		
		// Join testing when there is more than two tables
		Statement queryeight= queries.get(8);
		QueryPlan join2 = new QueryPlan(queryeight,DatabaseCatalog.getInstance());
		Operator joinTesting2= join2.getOperator();
		assertTrue(joinTesting2 instanceof JoinOperator);
		
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,156,156,142,9");
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,156,156,94,121");
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,156,156,193,12");
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,156,156,31,32");
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,70,70,75,147");
		
		// Test if the reset method works for this join
		joinTesting2.reset();
		
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,156,156,142,9");
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,156,156,94,121");
		assertEquals(joinTesting2.getNextTuple().toString(),"64,113,139,64,156,156,193,12");
		
		// Testing a join with a projection on top of it.
		Statement querysixteen= queries.get(16);
		QueryPlan projectionJoin = new QueryPlan(querysixteen,DatabaseCatalog.getInstance());
		Operator projectionJoinTesting= projectionJoin.getOperator();
		assertTrue(projectionJoinTesting instanceof ProjectOperator);
		
		assertEquals(projectionJoinTesting.getNextTuple().toString(),"9,156,64,64");
		assertEquals(projectionJoinTesting.getNextTuple().toString(),"121,156,64,64");
		assertEquals(projectionJoinTesting.getNextTuple().toString(),"12,156,64,64");
		assertEquals(projectionJoinTesting.getNextTuple().toString(),"32,156,64,64");
		assertEquals(projectionJoinTesting.getNextTuple().toString(),"147,70,64,64");
		
		// Test to see if the reset method works for this projectionjoin
		projectionJoinTesting.reset();
		assertEquals(projectionJoinTesting.getNextTuple().toString(),"9,156,64,64");
		
		
		// Sort testing 
		Statement queryseventeen = queries.get(17);
		QueryPlan sortJoin = new QueryPlan(queryseventeen,DatabaseCatalog.getInstance());
		Operator sortTesting= sortJoin.getOperator();
		
		// Test to see if the main operator is the sortOperator
		// Sort Operator takes a while and for some reason takes a while to compile 
		assertTrue(sortTesting instanceof SortOperator);

		assertEquals(sortTesting.getNextTuple().toString(),"4,77,1,4,89,89,52,115");
		assertEquals(sortTesting.getNextTuple().toString(),"4,77,1,4,89,89,66,152");
		assertEquals(sortTesting.getNextTuple().toString(),"4,77,1,4,89,89,103,172");
		assertEquals(sortTesting.getNextTuple().toString(),"4,77,1,4,89,89,154,196");
		assertEquals(sortTesting.getNextTuple().toString(),"4,77,1,4,90,90,75,158");
		
		// Testing reset for the sortoperators
		sortTesting.reset();
		assertEquals(sortTesting.getNextTuple().toString(),"4,77,1,4,89,89,52,115");

		// Testing duplicate elimination operator
		Statement queryten= queries.get(10);
		QueryPlan duplicate = new QueryPlan(queryten,DatabaseCatalog.getInstance());
		Operator duplicateTesting= duplicate.getOperator();
		assertTrue(duplicateTesting instanceof DuplicateEliminationOperator);
		
		assertEquals(duplicateTesting.getNextTuple().toString(),"0,47,120");
		assertEquals(duplicateTesting.getNextTuple().toString(),"0,49,176");
		assertEquals(duplicateTesting.getNextTuple().toString(),"0,58,191");
		assertEquals(duplicateTesting.getNextTuple().toString(),"0,97,129");
		assertEquals(duplicateTesting.getNextTuple().toString(),"0,135,109");
		
		duplicateTesting.reset();
		assertEquals(duplicateTesting.getNextTuple().toString(),"0,47,120");






		
		

		
		

		
		
		
		


		

		
		
		
		
		
		
		
		
	}
}
