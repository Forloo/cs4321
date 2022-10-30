//package test;
//
//import static org.junit.Assert.assertTrue;
//
//import java.io.File;
//import java.io.FileReader;
//import java.util.ArrayList;
//
//import org.junit.jupiter.api.Test;
//
//import net.sf.jsqlparser.parser.CCJSqlParser;
//import net.sf.jsqlparser.statement.Statement;
//import p1.logicaloperator.LogicalFilter;
//import p1.logicaloperator.LogicalJoin;
//import p1.logicaloperator.LogicalOperator;
//import p1.logicaloperator.LogicalProject;
//import p1.logicaloperator.LogicalScan;
//import p1.logicaloperator.LogicalSort;
//import p1.logicaloperator.LogicalUnique;
//import p1.util.DatabaseCatalog;
//import p1.util.LogicalPlan;
//
//public class LogicalPlanTest {
//
//	String queriesFile = "input" + File.separator + "queries.sql";
//	String queriesOutput = "output";
//	String dataDir = "input" + File.separator + "db" + File.separator;
//
//	// Get the file list containing all file inputs
//	File inputDir = new File(dataDir + "data");
//	String[] allFiles = inputDir.list();
//	File[] fileList = new File[allFiles.length];
//	File schema = new File(dataDir + "schema.txt");
//	String tempDir = "././temp";
//	File configFile = new File("././input/plan_builder_config.txt");
//
//	@Test
//	public void Logicaltesting() {
//		for (int i = 0; i < allFiles.length; i++) {
//			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
//			fileList[i] = file;
//		}
//
//		DatabaseCatalog.getInstance(fileList, schema, configFile, tempDir);
//
//		ArrayList<Statement> queries = new ArrayList<Statement>();
//
//		// Parse all of the queries and put them into the arraylist.
//
//		try {
//			CCJSqlParser parser = new CCJSqlParser(new FileReader("././input/queries.sql"));
//			Statement statement;
//			int queryCount = 1;
//			while ((statement = parser.Statement()) != null) {
//				try {
//					// Parse statement
//					queries.add(statement);
//				} catch (Exception e) {
//					System.err.println("Exception occurred during query " + queryCount);
//					e.printStackTrace();
//				}
//				queryCount++;
//			}
//		} catch (Exception err) {
//			System.out.println("The file you are looking for was not found");
//		}
//
//		// Test that the scan operator does work and that the type of that operator
//		// is of type scan
//		Statement queryZero = queries.get(0);
//		LogicalPlan scan = new LogicalPlan(queryZero);
//		LogicalOperator scantest = scan.getOperator();
//		assertTrue(scantest instanceof LogicalScan);
//
//		// Testing to see if the select is the filter type
//		Statement queryFour = queries.get(4);
//		LogicalPlan filter = new LogicalPlan(queryFour);
//		LogicalFilter filtertest = (LogicalFilter) filter.getOperator();
//		assertTrue(filtertest instanceof LogicalFilter);
//		assertTrue(filtertest.getChild() instanceof LogicalScan);
//
//		Statement queryOne = queries.get(1);
//		LogicalPlan project = new LogicalPlan(queryOne);
//		LogicalProject projectTesting = (LogicalProject) project.getOperator();
//		assertTrue(projectTesting instanceof LogicalProject);
//		// The child of this should be a scan
//		assertTrue(projectTesting.getChild() instanceof LogicalScan);
//
//		// Test to see if the project operator has the filter child when the
//		// operator
//		Statement queryFive = queries.get(5);
//		LogicalPlan project2 = new LogicalPlan(queryFive);
//		LogicalProject projectTesting2 = (LogicalProject) project2.getOperator();
//		assertTrue(projectTesting2 instanceof LogicalProject);
//		assertTrue(projectTesting2.getChild() instanceof LogicalFilter);
//		LogicalFilter filtering = (LogicalFilter) projectTesting2.getChild();
//		assertTrue(filtering.getChild() instanceof LogicalScan);
//
//		// Join testing
//		Statement querySeven = queries.get(7);
//		LogicalPlan join = new LogicalPlan(querySeven);
//		LogicalJoin jointesting = (LogicalJoin) join.getOperator();
//		assertTrue(jointesting instanceof LogicalJoin);
//
//		LogicalScan leftChild = (LogicalScan) jointesting.getLeftChild();
//		LogicalScan rightChild = (LogicalScan) jointesting.getRightChild();
//		assertTrue(leftChild instanceof LogicalScan);
//		assertTrue(rightChild instanceof LogicalScan);
//
//		// Testing the LogicalUnique
//		Statement queryten = queries.get(10);
//		LogicalPlan unique = new LogicalPlan(queryten);
//		LogicalUnique testingunique = (LogicalUnique) unique.getOperator();
//		assertTrue(testingunique instanceof LogicalUnique);
//
//		// The child for this unique should be just the sort operator since all
//		// Logical unique have the sort as their child
//		LogicalSort uniquechild = testingunique.getChild();
//		assertTrue(uniquechild instanceof LogicalSort);
//		assertTrue(uniquechild.getChild() instanceof LogicalScan);
//
//	}
//
//}
