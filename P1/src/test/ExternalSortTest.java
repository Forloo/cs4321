//package test;
//
//import java.io.File;
//import java.io.FileReader;
//import java.util.ArrayList;
//
//import org.junit.Test;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//
//
//import net.sf.jsqlparser.parser.CCJSqlParser;
//import net.sf.jsqlparser.statement.Statement;
//import p1.operator.BNLJOperator;
//import p1.operator.ExternalSortOperator;
//import p1.operator.Operator;
//import p1.util.DatabaseCatalog;
//import p1.util.QueryPlan;
//import p1.util.Tuple;
//
//public class ExternalSortTest {
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
//	void ExternalSortTest() {
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
//		Statement queryOne = queries.get(1);
//		DatabaseCatalog.getInstance().setSortMethod(1); 
//		DatabaseCatalog.getInstance().setSortPages(4); // external sort with 4-page sort buffers
//		QueryPlan planOne = new QueryPlan(queryOne, DatabaseCatalog.getInstance());
//		ExternalSortOperator sortOne= (ExternalSortOperator) planOne.getOperator();
//		
//		System.out.println(sortOne.getNextTuple());
//
//		
//	}
//}
