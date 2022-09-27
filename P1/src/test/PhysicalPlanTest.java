package test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.util.DatabaseCatalog;
import p1.util.LogicalPlan;
import p1.util.PhysicalPlanBuilder;
import p1.util.QueryTree;
import p1.util.QueryTreePlan;

public class PhysicalPlanTest {
	
	
	String queriesFile = "input" + File.separator + "queries.sql";
	String queriesOutput = "output";
	String dataDir = "input" + File.separator + "db" + File.separator;

	// Get the file list containing all file inputs
	File inputDir = new File(dataDir + "data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	File schema = new File(dataDir + "schema.txt");
	
	@Test
	public void conversionTesting() {
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
		
		Statement first = queries.get(0);
		Select firstSelect= (Select) first;
		PlainSelect plainSelect = (PlainSelect) firstSelect.getSelectBody();
		LogicalPlan firstlp= new LogicalPlan(first);
		PhysicalPlanBuilder firstpp= new PhysicalPlanBuilder(plainSelect);
		firstlp.accept(firstpp);
		QueryTreePlan firstQTP = firstpp.getPlan();
		QueryTree firstQT= firstQTP.getTree();
		String nodeOrdering=firstQT.toString(firstQT.getRoot());
		// There should be only the scan node and that is correct
//		System.out.println(nodeOrdering);
		
		
		Statement second = queries.get(1);
		Select secondSelect= (Select) second;
		PlainSelect plainSelect2 = (PlainSelect) secondSelect.getSelectBody();
		LogicalPlan secondlp= new LogicalPlan(second);
		PhysicalPlanBuilder secondpp= new PhysicalPlanBuilder(plainSelect2);
		secondlp.accept(secondpp);
		QueryTreePlan secondQTP = secondpp.getPlan();
		QueryTree secondQT= secondQTP.getTree();
		String secondnodeOrdering=secondQT.toString(secondQT.getRoot());
		// The expected result for this is the project operator then followed by the scan.
//		System.out.println(secondnodeOrdering);
		
		Statement fifth= queries.get(4);
		Select fifthSelect= (Select) fifth;
		PlainSelect plainSelect5= (PlainSelect) fifthSelect.getSelectBody();
		LogicalPlan fifthlp= new LogicalPlan(fifth);
		PhysicalPlanBuilder fifthpp = new PhysicalPlanBuilder(plainSelect5);
		fifthlp.accept(fifthpp);
		QueryTreePlan fifthQTP = fifthpp.getPlan();
		QueryTree fifthQT = fifthQTP.getTree();
		String fifthnodeOrdering= fifthQT.toString(fifthQT.getRoot());
		// The result that is expected is a select. 
		//I set select to be a leaf. We can change that if needed after doing refactoring
		//System.out.println(fifthnodeOrdering);
		
		Statement sixth= queries.get(5);
		Select sixthSelect= (Select) sixth;
		PlainSelect plainSelect6= (PlainSelect) sixthSelect.getSelectBody();
		LogicalPlan sixthlp= new LogicalPlan(sixth);
		PhysicalPlanBuilder sixthpp = new PhysicalPlanBuilder(plainSelect6);
		sixthlp.accept(sixthpp);
		QueryTreePlan sixthQTP = sixthpp.getPlan();
		QueryTree sixthQT = sixthQTP.getTree();
		String sixthordering= sixthQT.toString(sixthQT.getRoot());
		// The result should be a project followed by a select.
//		System.out.println(sixthordering);
		
		Statement seventh= queries.get(7);
		Select seventhSelect =(Select) seventh;
		PlainSelect plainSelect7= (PlainSelect) seventhSelect.getSelectBody();
		LogicalPlan seventhlp=new LogicalPlan(seventh);
		PhysicalPlanBuilder seventhpp= new PhysicalPlanBuilder(plainSelect7);
		seventhlp.accept(seventhpp);
		QueryTreePlan seventhQTP= seventhpp.getPlan();
		QueryTree seventhQT =seventhQTP.getTree();
		String seventhOrdering= seventhQT.toString(seventhQT.getRoot());
		// The result should be a projection and then we should have two joins right after that.
		System.out.println(seventhOrdering);
		
		
	}
	
}
