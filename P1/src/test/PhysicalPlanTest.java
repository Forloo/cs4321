package test;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import p1.operator.DuplicateEliminationOperator;
import p1.operator.TNLJOperator;
import p1.operator.Operator;
import p1.operator.ProjectOperator;
import p1.operator.ScanOperator;
import p1.operator.SelectOperator;
import p1.operator.SortOperator;
import p1.util.DatabaseCatalog;
import p1.util.LogicalPlan;
import p1.util.PhysicalPlanBuilder;
import p1.util.QueryPlan;

public class PhysicalPlanTest {

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
	public void conversionTesting() {
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

		// Test all of the queries that we tested in queryplan
		Statement queryZero = queries.get(0);
		PhysicalPlanBuilder builder1 = new PhysicalPlanBuilder(queryZero);
		LogicalPlan lp1 = new LogicalPlan(queryZero);
		lp1.accept(builder1);
		QueryPlan p1 = builder1.getPlan();

		// Check if the operator is a scan
		Operator p1root = p1.getOperator();
		assertTrue(p1root instanceof ScanOperator);

		assertEquals(p1root.getNextTuple().toString(), "64,113,139");
		assertEquals(p1root.getNextTuple().toString(), "181,128,129");
		assertEquals(p1root.getNextTuple().toString(), "147,45,118");
		assertEquals(p1root.getNextTuple().toString(), "81,1,195");
		assertEquals(p1root.getNextTuple().toString(), "75,191,192");

		p1root.reset();
		assertEquals(p1root.getNextTuple().toString(), "64,113,139");

		// Testing when the operator is select
		Statement queryFour = queries.get(4);
		PhysicalPlanBuilder builderfour = new PhysicalPlanBuilder(queryFour);
		LogicalPlan lp4 = new LogicalPlan(queryFour);
		lp4.accept(builderfour);
		QueryPlan p4 = builderfour.getPlan();

		// Check if the operator is select
		Operator p4root = p4.getOperator();
		assertTrue(p4root instanceof SelectOperator);

		assertEquals(p4root.getNextTuple().toString(), "133,197,18");
		assertEquals(p4root.getNextTuple().toString(), "157,148,100");
		assertEquals(p4root.getNextTuple().toString(), "118,72,25");
		assertEquals(p4root.getNextTuple().toString(), "5,119,15");
		assertEquals(p4root.getNextTuple().toString(), "173,194,157");

		p4root.reset();

		assertEquals(p4root.getNextTuple().toString(), "133,197,18");

		// Testing the project method when the child is the scan
		Statement queryOne = queries.get(1);
		PhysicalPlanBuilder p1builder = new PhysicalPlanBuilder(queryOne);
		LogicalPlan lp1_ = new LogicalPlan(queryOne);
		lp1_.accept(p1builder);
		QueryPlan p1plan = p1builder.getPlan();
		Operator p1root_ = p1plan.getOperator();
		assertTrue(p1root_ instanceof ProjectOperator);

		assertEquals(p1root_.getNextTuple().toString(), "64");
		assertEquals(p1root_.getNextTuple().toString(), "181");
		assertEquals(p1root_.getNextTuple().toString(), "147");
		assertEquals(p1root_.getNextTuple().toString(), "81");
		assertEquals(p1root_.getNextTuple().toString(), "75");

		p1root_.reset();

		assertEquals(p1root_.getNextTuple().toString(), "64");

		// Test project when the child is the select
		Statement queryFive = queries.get(5);
		PhysicalPlanBuilder builderFive = new PhysicalPlanBuilder(queryFive);
		LogicalPlan lp5 = new LogicalPlan(queryFive);
		lp5.accept(builderFive);
		QueryPlan p5 = builderFive.getPlan();
		Operator p5root = p5.getOperator();
		assertTrue(p5root instanceof ProjectOperator);

		assertEquals(p5root.getNextTuple().toString(), "133");
		assertEquals(p5root.getNextTuple().toString(), "157");
		assertEquals(p5root.getNextTuple().toString(), "118");
		assertEquals(p5root.getNextTuple().toString(), "5");
		assertEquals(p5root.getNextTuple().toString(), "173");

		p5root.reset();
		assertEquals(p5root.getNextTuple().toString(), "133");

		// Join testing
		Statement querySeven = queries.get(7);
		PhysicalPlanBuilder builderSeven = new PhysicalPlanBuilder(querySeven);
		LogicalPlan lp7 = new LogicalPlan(querySeven);
		lp7.accept(builderSeven);
		QueryPlan p7 = builderSeven.getPlan();
		Operator p7root = p7.getOperator();
		assertTrue(p7root instanceof TNLJOperator);

		assertEquals(p7root.getNextTuple().toString(), "64,113,139,64,156");
		assertEquals(p7root.getNextTuple().toString(), "64,113,139,64,70");
		assertEquals(p7root.getNextTuple().toString(), "64,113,139,64,170");
		assertEquals(p7root.getNextTuple().toString(), "64,113,139,64,16");
		assertEquals(p7root.getNextTuple().toString(), "64,113,139,64,70");

		// Test if the reset works
		p7root.reset();

		assertEquals(p7root.getNextTuple().toString(), "64,113,139,64,156");
		assertEquals(p7root.getNextTuple().toString(), "64,113,139,64,70");

		// Test the join operator when there is more than two tables
		Statement queryeight = queries.get(8);
		PhysicalPlanBuilder builderEight = new PhysicalPlanBuilder(queryeight);
		LogicalPlan lp8 = new LogicalPlan(queryeight);
		lp8.accept(builderEight);
		QueryPlan p8 = builderEight.getPlan();
		Operator p8root = p8.getOperator();
		assertTrue(p8root instanceof TNLJOperator);

		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,156,156,142,9");
		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,156,156,94,121");
		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,156,156,193,12");
		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,156,156,31,32");
		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,70,70,75,147");

		p8root.reset();

		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,156,156,142,9");
		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,156,156,94,121");
		assertEquals(p8root.getNextTuple().toString(), "64,113,139,64,156,156,193,12");

		// Testing the join when there is projection on top of it
		Statement querysixteen = queries.get(16);
		PhysicalPlanBuilder buildersixteen = new PhysicalPlanBuilder(querysixteen);
		LogicalPlan lp16 = new LogicalPlan(querysixteen);
		lp16.accept(buildersixteen);
		QueryPlan p16 = buildersixteen.getPlan();
		Operator p16root = p16.getOperator();
		assertTrue(p16root instanceof ProjectOperator);

		assertEquals(p16root.getNextTuple().toString(), "9,156,64,64");
		assertEquals(p16root.getNextTuple().toString(), "121,156,64,64");
		assertEquals(p16root.getNextTuple().toString(), "12,156,64,64");
		assertEquals(p16root.getNextTuple().toString(), "32,156,64,64");
		assertEquals(p16root.getNextTuple().toString(), "147,70,64,64");

		p16root.reset();
		assertEquals(p16root.getNextTuple().toString(), "9,156,64,64");

		// Sort testing
		Statement queryseventeen = queries.get(17);
		PhysicalPlanBuilder builderseventeen = new PhysicalPlanBuilder(queryseventeen);
		LogicalPlan lp17 = new LogicalPlan(queryseventeen);
		lp17.accept(builderseventeen);
		QueryPlan p17 = builderseventeen.getPlan();
		Operator p17root = p17.getOperator();
		assertTrue(p17root instanceof SortOperator);

		assertEquals(p17root.getNextTuple().toString(), "4,77,1,4,89,89,52,115");
		assertEquals(p17root.getNextTuple().toString(), "4,77,1,4,89,89,66,152");
		assertEquals(p17root.getNextTuple().toString(), "4,77,1,4,89,89,103,172");
		assertEquals(p17root.getNextTuple().toString(), "4,77,1,4,89,89,154,196");
		assertEquals(p17root.getNextTuple().toString(), "4,77,1,4,90,90,75,158");

		// Testing reset for the sortoperators
		p17root.reset();
		assertEquals(p17root.getNextTuple().toString(), "4,77,1,4,89,89,52,115");

		Statement queryten = queries.get(10);
		PhysicalPlanBuilder builderten = new PhysicalPlanBuilder(queryten);
		LogicalPlan lp10 = new LogicalPlan(queryten);
		lp10.accept(builderten);
		QueryPlan p10 = builderten.getPlan();
		Operator p10root = p10.getOperator();
		assertTrue(p10root instanceof DuplicateEliminationOperator);

		assertEquals(p10root.getNextTuple().toString(), "0,47,120");
		assertEquals(p10root.getNextTuple().toString(), "0,49,176");
		assertEquals(p10root.getNextTuple().toString(), "0,58,191");
		assertEquals(p10root.getNextTuple().toString(), "0,97,129");
		assertEquals(p10root.getNextTuple().toString(), "0,135,109");

		p10root.reset();
		assertEquals(p10root.getNextTuple().toString(), "0,47,120");

	}

}
