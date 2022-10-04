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
import p1.logicaloperator.LogicalFilter;
import p1.logicaloperator.LogicalJoin;
import p1.logicaloperator.LogicalOperator;
import p1.logicaloperator.LogicalProject;
import p1.logicaloperator.LogicalScan;
import p1.logicaloperator.LogicalSort;
import p1.logicaloperator.LogicalUnique;
import p1.util.DatabaseCatalog;
import p1.util.LogicalNode;
import p1.util.LogicalPlan;
import p1.util.LogicalTree;

public class LogicalPlanTest {

	String queriesFile = "input" + File.separator + "queries.sql";
	String queriesOutput = "output";
	String dataDir = "input" + File.separator + "db" + File.separator;

	// Get the file list containing all file inputs
	File inputDir = new File(dataDir + "data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	File schema = new File(dataDir + "schema.txt");
	
	@Test
	public void Logicaltesting() {
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
		
		// Test that the scan operator does work and that the type of that operator 
		// is of type scan
		Statement queryZero = queries.get(0);
		LogicalPlan scan = new LogicalPlan(queryZero);
		LogicalOperator scantest = scan.getOperator();
		assertTrue(scantest instanceof  LogicalScan);
		
		// Testing to see if the select is the filter type
		Statement queryFour = queries.get(4);
		LogicalPlan filter =new LogicalPlan(queryFour);
		LogicalFilter filtertest = (LogicalFilter)filter.getOperator();
		assertTrue(filtertest instanceof LogicalFilter);
		assertTrue(filtertest.getChild() instanceof LogicalScan);
		
		Statement queryOne = queries.get(1);
		LogicalPlan project = new LogicalPlan(queryOne);
		LogicalProject projectTesting = (LogicalProject) project.getOperator();
		assertTrue(projectTesting instanceof LogicalProject);
		// The child of this should be a scan
		assertTrue(projectTesting.getChild() instanceof LogicalScan);
		
		// Test to see if the project operator has the filter child when the 
		// operator 
		Statement queryFive = queries.get(5);
		LogicalPlan project2= new LogicalPlan(queryFive);
		LogicalProject projectTesting2=(LogicalProject) project2.getOperator();
		assertTrue(projectTesting2 instanceof LogicalProject);
		assertTrue(projectTesting2.getChild() instanceof LogicalFilter);
		LogicalFilter filtering = (LogicalFilter) projectTesting2.getChild();
		assertTrue(filtering.getChild() instanceof LogicalScan);
		
		// Join testing
		Statement querySeven = queries.get(7);
		LogicalPlan join = new LogicalPlan(querySeven);
		LogicalJoin jointesting=(LogicalJoin) join.getOperator();
		assertTrue(jointesting instanceof LogicalJoin);
		
		LogicalScan leftChild= (LogicalScan)jointesting.getLeftChild();
		LogicalScan rightChild= (LogicalScan) jointesting.getRightChild();
		assertTrue(leftChild instanceof LogicalScan);
		assertTrue(rightChild instanceof LogicalScan);
		
		// Testing a join with a project on top of it
		Statement querysixteen= queries.get(16);
		LogicalPlan join2= new LogicalPlan(querysixteen);
		// Get the projectoperator root 
		LogicalProject projectRoot= (LogicalProject)join2.getOperator();
		assertTrue(projectRoot instanceof LogicalProject);
	
		// the child of this is a join
		LogicalJoin child1= (LogicalJoin)projectRoot.getChild();
		assertTrue(child1 instanceof LogicalJoin);
		
		// The left child of this is a join and then the right child of this
		// node is a scan operator since each child does not have a expression for itself
		LogicalJoin child2left = (LogicalJoin) child1.getLeftChild();
		LogicalScan child2right= (LogicalScan) child1.getRightChild();
		assertTrue(child2left instanceof LogicalJoin);
		assertTrue(child2right instanceof LogicalScan);
		
		// The left child of the child2left should be a scan and so should the right 
		// child of the child2left.
		LogicalScan child3left= (LogicalScan) child2left.getLeftChild();
		LogicalScan child3right= (LogicalScan) child2left.getRightChild();
		assertTrue(child3left instanceof LogicalScan);
		assertTrue(child3right instanceof LogicalScan);
		
		
		// Testing if the sort logical operator will work
		Statement query17 = queries.get(17);
		LogicalPlan sort= new LogicalPlan(query17);
		LogicalSort sorttesting= (LogicalSort) sort.getOperator();
		assertTrue(sorttesting instanceof LogicalSort);
		
		// The first child of the sort should be a logicalJoin
		LogicalJoin firstchild= (LogicalJoin) sorttesting.getChild();
		assertTrue(firstchild instanceof LogicalJoin);
		
		// The left child should be a join
		// The right child should be a scan
		LogicalJoin secondchildleft= (LogicalJoin) firstchild.getLeftChild();
		LogicalScan secondchildright= (LogicalScan) firstchild.getRightChild();
		assertTrue(secondchildleft instanceof LogicalJoin);
		assertTrue(secondchildright instanceof LogicalScan);
		
		// Both childs are scans
		assertTrue(secondchildleft.getLeftChild() instanceof LogicalScan);
		assertTrue(secondchildleft.getRightChild() instanceof LogicalScan);
		
		// Testing the LogicalUnique
		Statement queryten= queries.get(10);
		LogicalPlan unique = new LogicalPlan(queryten);
		LogicalUnique testingunique= (LogicalUnique) unique.getOperator();
		assertTrue(testingunique instanceof LogicalUnique);
		
		// The child for this unique should be just the sort operator since all
		// Logical unique have the sort as their child
		LogicalSort uniquechild = (LogicalSort) testingunique.getChild();
		assertTrue(uniquechild instanceof LogicalSort);
		assertTrue(uniquechild.getChild() instanceof LogicalScan);

		
		
	}
	
}
