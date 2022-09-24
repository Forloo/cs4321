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
		
		// Get the first element in the query
		Statement first = queries.get(0);
		Select select = (Select) first;
		PlainSelect plainselect = (PlainSelect) select.getSelectBody();
		LogicalTree temp = new LogicalTree();
		
		LogicalNode root= temp.buildTree(plainselect);
		temp.setRoot(root);
		
		// Check if there is only one scan node in this tree
//		temp.dfs(temp.getRoot());	
		
		
		// Get the second element in the query
		Statement second= queries.get(1);
		Select select2 = (Select) second;
		PlainSelect plainselect2 = (PlainSelect) select2.getSelectBody();
		LogicalTree tree2= new LogicalTree();
		LogicalNode root2= tree2.buildTree(plainselect2);
		tree2.setRoot(root2);
		
		// Checking to see if there is the projection node here and the scan node
//		tree2.dfs(root2);
		
		
		// Get the fourth element in the queyr
		Statement fourth = queries.get(4);
		Select select4 = (Select) fourth;
		PlainSelect plainselect4= (PlainSelect) select4.getSelectBody();
		LogicalTree tree4= new LogicalTree();
		LogicalNode root4= tree4.buildTree(plainselect4);
		tree4.setRoot(root4);
		
		// Check if there is only a logical filter node since that is one of the bases cases.
//		tree4.dfs(root4);
		
		// Get the fifth elemnt in the query
		Statement fifth= queries.get(5);
		Select select5= (Select) fifth;
		PlainSelect plainselect5 = (PlainSelect) select5.getSelectBody();
		LogicalTree tree5= new LogicalTree();
		LogicalNode root5= tree5.buildTree(plainselect5);
		tree5.setRoot(root5);
		
		// Check if there is a projection node followed by a filter node
//		tree5.dfs(root5);
		
		Statement seventh = queries.get(7);
		Select select7 = (Select) seventh;
		PlainSelect plainSelect7 = (PlainSelect) select7.getSelectBody();
		LogicalTree tree7 = new LogicalTree();
		LogicalNode root7 = tree7.buildTree(plainSelect7);
		tree7.setRoot(root7);
		
		// There should be only one node. It should be a join node.
		//tree7.dfs(root7);
		
		Statement eight= queries.get(8);
		Select select8 = (Select) eight;
		PlainSelect plainselect8= (PlainSelect) select8.getSelectBody();
		LogicalTree tree8 = new LogicalTree();
		LogicalNode root8 = tree8.buildTree(plainselect8);
		tree8.setRoot(root7);
		
		// There should be two join nodes. One for the first two tables.
		// Then another for the first two tables combined and the next table.
//		tree8.dfs(root8);
		
		// Do a test where there is a projection or distinct node in front of joins
		Statement fifteen= queries.get(15);
		Select select15 = (Select) fifteen;
		PlainSelect plainselect15 = (PlainSelect) select15.getSelectBody();
		LogicalTree tree15 = new LogicalTree();
		LogicalNode root15 = tree15.buildTree(plainselect15);
		tree15.setRoot(root15);
		
		// The expected output for this is two join nodes and then the root node
		// is the projection operator.
//		tree15.dfs(root15);
		
		// Do a test where there is a distinct node
		Statement fourteenth = queries.get(14);
		Select select14= (Select) fourteenth;
		PlainSelect plainselect14 = (PlainSelect) select14.getSelectBody();
		LogicalPlan plan = new LogicalPlan(fourteenth);
		
		ArrayList<LogicalNode> allNodes= plan.getOperators(plan.getRoot());
		for(int i=0;i<allNodes.size();i++) {
			System.out.println(allNodes.get(i));
		}
		
		
	}
	
}
