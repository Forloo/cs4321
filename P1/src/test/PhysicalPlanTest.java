package test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import static org.junit.Assert.assertTrue;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.JoinOperator;
import p1.operator.ProjectOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;
import p1.util.DatabaseCatalog;
import p1.util.LogicalPlan;
import p1.util.PhysicalPlanBuilder;
import p1.util.QueryPlan;
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
		
		// Grab the first query 
		 Statement first = queries.get(0);
		 LogicalPlan firstlp = new LogicalPlan(first);
		 Select firstselect = (Select) first;
		 PlainSelect plainSelect = (PlainSelect) firstselect.getSelectBody();
		 PhysicalPlanBuilder firstpb= new PhysicalPlanBuilder(plainSelect);
		 firstlp.accept(firstpb);
		 QueryPlan firstPlan= firstpb.getPlan();
		 assertTrue(firstPlan.getOperator() instanceof ScanOperator);
		 
		 // Grab the second query
		 Statement second = queries.get(1);
		 LogicalPlan secondlp= new LogicalPlan(second);
		 Select secondselect = (Select) second;
		 PlainSelect plainSelect2= (PlainSelect) secondselect.getSelectBody();
		 PhysicalPlanBuilder secondpb= new PhysicalPlanBuilder(plainSelect2);
		 secondlp.accept(secondpb);
		 QueryPlan secondPlan= secondpb.getPlan();
		 assertTrue(secondPlan.getOperator() instanceof ProjectOperator);
		 
		 // Grab the fourth query
		 Statement fourth = queries.get(4);
		 LogicalPlan fourthlp= new LogicalPlan(fourth);
		 Select fourthselect = (Select) fourth;
		 PlainSelect plainSelect4 = (PlainSelect) fourthselect.getSelectBody();
		 PhysicalPlanBuilder fourthpb= new PhysicalPlanBuilder(plainSelect4);
		 fourthlp.accept(fourthpb);
		 QueryPlan fourthPlan = fourthpb.getPlan();
		 assertTrue(fourthPlan.getOperator() instanceof SelectOperator);
		 
		 // Grab the fifth query it should be a project
		 Statement fifth= queries.get(5);
		 LogicalPlan fifthlp= new LogicalPlan(fifth);
		 Select fifthselect = (Select) fifth;
		 PlainSelect plainSelect5= (PlainSelect) fifthselect.getSelectBody();
		 PhysicalPlanBuilder fifthpb= new PhysicalPlanBuilder(plainSelect5);
		 fifthlp.accept(fifthpb);
		 QueryPlan fifthPlan= fifthpb.getPlan();
		 assertTrue(fifthPlan.getOperator() instanceof ProjectOperator);
		 
		 // Grab the seventh query it should be a join
		 Statement seventh = queries.get(7);
		 LogicalPlan seventhlp = new LogicalPlan(seventh);
		 Select seventhselect = (Select) seventh;
		 PlainSelect plainSelect7= (PlainSelect) seventhselect.getSelectBody();
		 PhysicalPlanBuilder seventhpb= new PhysicalPlanBuilder(plainSelect7);
		 seventhlp.accept(seventhpb);
		 QueryPlan seventhPlan= seventhpb.getPlan();
		 assertTrue(seventhPlan.getOperator() instanceof JoinOperator);
		
		 // Grab the tenth element it should be a distinct
		 Statement tenth = queries.get(10);
		 LogicalPlan tenthlp = new LogicalPlan(tenth);
		 Select tenthselect= (Select) tenth;
		 PlainSelect plainSelect10 = (PlainSelect) tenthselect.getSelectBody();
		 PhysicalPlanBuilder tenthpb= new PhysicalPlanBuilder(plainSelect10);
		 tenthlp.accept(tenthpb);
		 QueryPlan tenthPlan = tenthpb.getPlan();
		 assertTrue(tenthPlan.getOperator() instanceof DuplicateEliminationOperator);
		 
		 // Grab the thirteenth element it should be distinct
		 Statement thirteenth = queries.get(13);
		 LogicalPlan thirteenthlp = new LogicalPlan(thirteenth);
		 Select thirteenthselect= (Select) thirteenth;
		 PlainSelect plainSelect13 = (PlainSelect) thirteenthselect.getSelectBody();
		 PhysicalPlanBuilder thirteenthpb= new PhysicalPlanBuilder(plainSelect13);
		 
		 QueryPlan example = new QueryPlan(thirteenth,DatabaseCatalog.getInstance());
		 
		 thirteenthlp.accept(thirteenthpb);
		 QueryPlan thirteenthplan = thirteenthpb.getPlan();
		 assertTrue(thirteenthplan.getOperator() instanceof SortOperator);
		 
		 
		
	}
	
}
