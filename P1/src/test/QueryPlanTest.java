package test;

import java.io.FileReader;
import java.util.ArrayList;
import java.io.File;
import java.io.FileNotFoundException;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.operator.ScanOperator;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import p1.QueryPlan;
import p1.databaseCatalog.*;

public class QueryPlanTest{
	
	String queriesFile = "input" + File.separator + "queries.sql";
	String queriesOutput = "output";
	String dataDir = "input" + File.separator + "db" + File.separator;

	// Get the file list containing all file inputs
	File inputDir = new File(dataDir + "data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	File schema = new File(dataDir + "schema.txt");
	
	@Test
	void QueryPlanTesting() {
		for (int i = 0; i < allFiles.length; i++) {
			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
			fileList[i] = file;
		}
		
		DatabaseCatalog.getInstance(fileList, schema);
		
		ArrayList<Statement> queries= new ArrayList<Statement>();
		
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

		
		// Cannot process all of the queries as we do not handle alias yet and retrieving 
		// information from more than one table. 
		// For now the expected total number of nodes is correct. Furthered testing 
		// need when select and project are finished.
		QueryPlan q1= new QueryPlan(queries.get(0),DatabaseCatalog.getInstance());
		System.out.println(q1);
		
		QueryPlan q2= new QueryPlan(queries.get(1),DatabaseCatalog.getInstance());
		System.out.println(q2);
		
}
}

