package test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.ExpressionParser;
import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;
import p1.operator.JoinOperator;

public class JoinOperatorTest {

	String queriesFile = "input" + File.separator + "queries.sql";
	String queriesOutput = "output";
	String dataDir = "input" + File.separator + "db" + File.separator;

	// Get the file list containing all file inputs
	File inputDir = new File(dataDir + "data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	File schema = new File(dataDir + "schema.txt");
	
	@Test
	public void JoinOperatorTesting() {
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
		
		// Get the fourth query that requires a join
		Statement fourth=queries.get(4);
		
		Select select=(Select) fourth;
		PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
		JoinOperator join = new JoinOperator(plainSelect,"",DatabaseCatalog.getInstance());
		
		
		// Test that the final schema table is correct (Test again after since we are renaming the tables)
		ArrayList<String> results= new ArrayList<String>();
		results.add("A");
		results.add("B");
		results.add("C");
		results.add("G");
		results.add("H");
		
		assertEquals(results,join.getSchema());
		join.dump();	
		Expression value=join.getWhere();
		
		ExpressionParser hope = new ExpressionParser(value);
		value.accept(hope);
		
		// Get the fourth query that requires a join
		Statement ninth=queries.get(9);
		
		Select select2=(Select) ninth;
		PlainSelect plainSelect2 = (PlainSelect)select2.getSelectBody();
		JoinOperator join2 = new JoinOperator(plainSelect2,"",DatabaseCatalog.getInstance());
		Expression value2= join2.getWhere();
		ExpressionParser hope2 = new ExpressionParser(value2);
		value2.accept(hope2);
		ArrayList<Expression> valueOne= hope2.getList();
		
		// Grab the tenth query
		Statement ten= queries.get(10);
		Select select3= (Select)ten;
		PlainSelect plainSelect3= (PlainSelect) select3.getSelectBody();
		JoinOperator join3= new JoinOperator(plainSelect3,"",DatabaseCatalog.getInstance());
		
		// Want to see the expected values from the leaf nodes
		
		
		
		
//		System.out.println(hope3.size());
//		for(String[] key: hope3.keySet()) {
//			for(int i=0;i<key.length;i++) {
//				System.out.print(key[i]);
//			}
//			System.out.println(hope3.get(key));
//		}
//		
		
//		System.out.println(value);
		
		// Three node parsing works
//		Statement threeTable = (Select) queries.get(0);
//		Select select = (Select) threeTable;
//		PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
//		JoinOperator join = new JoinOperator(plainSelect,"");
//		JoinOperatorTree tree= join.getRoot();
//		tree.dfs(tree.getRoot());
		
		
	}
	
	
}
