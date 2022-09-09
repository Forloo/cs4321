package test;

import java.io.FileReader;
import java.util.ArrayList;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.operator.ScanOperator;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import p1.databaseCatalog.*;
import p1.QueryPlan2;

public class QueryPlan2Test {
	
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
		
		// Make the queryplan object
		QueryPlan2 scanRoot= new QueryPlan2(queries.get(0),DatabaseCatalog.getInstance());
		
		// Check if the expected output for scan is still working as intended.q
		assertEquals("1,200,50", scanRoot.getOperator().getNextTuple().toString());
		assertEquals("2,200,200", scanRoot.getOperator().getNextTuple().toString());
		assertEquals("3,100,105", scanRoot.getOperator().getNextTuple().toString());
		assertEquals("4,100,50", scanRoot.getOperator().getNextTuple().toString());
		assertEquals("5,100,500", scanRoot.getOperator().getNextTuple().toString());
		assertEquals("6,300,400", scanRoot.getOperator().getNextTuple().toString());
		assertNull(scanRoot.getOperator().getNextTuple());
		assertNull(scanRoot.getOperator().getNextTuple());
		
		// Check if the reset method for scan still works.
		scanRoot.getOperator().reset();

		assertEquals("1,200,50", scanRoot.getOperator().getNextTuple().toString());
		
	}
}
