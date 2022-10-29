package p1;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.index.BTree;
import p1.index.BTreeNode;
import p1.io.BPTreeReader;
import p1.io.BPTreeWriter;
import p1.io.BPTreeReader;
import p1.io.BinaryTupleWriter;
import p1.io.FileConverter;
import p1.util.DatabaseCatalog;
import p1.util.LogicalPlan;
import p1.util.PhysicalPlanBuilder;
import p1.util.QueryPlan;
import p1.util.RandomDataGenerator;
import p1.util.SortFile;
import p1.util.Tuple;

public class Main {

	public static void main(String[] args) {
		String queriesFile = args[0] + File.separator + "queries.sql";
		String queriesOutput = args[1];
		String dataDir = args[0] + File.separator + "db" + File.separator;
		String tempDir = args[2] + File.separator;

		// Get the file list containing all file inputs
		File inputDir = new File(dataDir + "data");
		String[] allFiles = inputDir.list();
		File[] fileList = new File[allFiles.length];
		File schema = new File(dataDir + "schema.txt");
		
		for (int i = 0; i < allFiles.length; i++) {
			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
			fileList[i] = file;
		}


//testing BPTreeWriter
//		File file = new File("/Users/jinseokoh/git/cs4321/P1/expected_indexes/testingBPTreeWriter");
//		BTree testingOne = new BTree(3,true,0,file,"/Users/jinseokoh/git/cs4321/P1/input/db/data/Sailors",0);
//		BTreeNode root = testingOne.constructTree();
////		System.out.println(testingOne.getAllLevels()); 
//		testingOne.setRoot(root);
//		System.out.println(testingOne.getRoot());
//		BPTreeWriter bptw = new BPTreeWriter(testingOne.getAllLevels(), file, testingOne.getRoot(),3);
//		
//		BPTreeReader btr = new BPTreeReader("/Users/jinseokoh/git/cs4321/P1/expected_indexes/Sailors.A");
//		System.out.println("Header Page info: tree has order " + btr.getOrderOfTree() + ", a root at address " + btr.getAddressOfRoot()+ " and " +btr.getNumLeaves() + " leaf nodes");
//		btr.checkNodeType();
//		btr.checkNodeType(); //try second leaf page
//		btr.getNextDataEntryUnclus();
//		btr.getNextDataEntryUnclus();
//		btr.getNextDataEntryUnclus();
//		btr.getNextDataEntryUnclus();
//		
//		int c = 0;
//		Boolean v = btr.checkNodeType();
//		while(v == true) {//reach index nodes
//			c += 1;
//			v = btr.checkNodeType();
//		}
//		btr.checkNodeType(); //try second page of index node
//		int btrKey= btr.getNextKey();
//		while(( btrKey  != -1)) {
//			System.out.println(btrKey);
//			btrKey = btr.getNextKey();
//		}
//		int child = btr.getNextAddrIN();
//		while ((child) != -1) {
//			System.out.println(child);
//			child = btr.getNextAddrIN();
//		}
//		
//		while(btr.checkNodeType() != null) {
//		}
//		System.out.println("works");
//testing BPTreeWriter
		
		

		

		
		
//		//debugging by generating random data
//		String fileName = "/Users/jocelynsun/Desktop/CS 4321/cs4321/P1/input/db/data/Boats";
//		ArrayList<Tuple> rdg = new RandomDataGenerator(3,10000).generate();
//		BinaryTupleWriter writer = new BinaryTupleWriter(fileName); 
//		for(Tuple t : rdg) {
//		    writer.writeTuple(t);
//		} writer.close();
//		//for debugging
//		FileConverter.convertBinToHuman(fileName, fileName + "_humanreadable");
//		String fileName2 = "/Users/jocelynsun/Desktop/CS 4321/cs4321/P1/input/db/data/Sailors";
//		ArrayList<Tuple> rdg2 = new RandomDataGenerator(3,10000).generate();
//		BinaryTupleWriter writer2 = new BinaryTupleWriter(fileName2); 
//		for(Tuple t : rdg2) {
//		    writer2.writeTuple(t);
//		} writer2.close();
//		//for debugging
//		FileConverter.convertBinToHuman(fileName2, fileName2 + "_humanreadable");
//		String fileName3 = "/Users/jocelynsun/Desktop/CS 4321/cs4321/P1/input/db/data/Reserves";
//		ArrayList<Tuple> rdg3 = new RandomDataGenerator(3,10000).generate();
//		BinaryTupleWriter writer3 = new BinaryTupleWriter(fileName3); 
//		for(Tuple t : rdg3) {
//		    writer3.writeTuple(t);
//		} writer3.close();
//		//for debugging
//		FileConverter.convertBinToHuman(fileName3, fileName3 + "_humanreadable");
//		
		
		DatabaseCatalog db = DatabaseCatalog.getInstance(fileList, schema,
				new File(args[0] + File.separator + "plan_builder_config.txt"), tempDir);

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			int queryCount = 1;

			while ((statement = parser.Statement()) != null) {
				try {
					// Parse statement
					Select select = (Select) statement;
					PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
					System.out.println(plainSelect);

					// Create results file in output file directory
					String queriesOutputFile = queriesOutput + File.separator + "query" + queryCount;
					File queryResult = new File(queriesOutputFile);
					queryResult.createNewFile();

					// Evaluate query
					LogicalPlan lp = new LogicalPlan(statement);
					PhysicalPlanBuilder builder = new PhysicalPlanBuilder(statement);
					lp.accept(builder);
					QueryPlan qp = builder.getPlan();
					long startMillis = System.currentTimeMillis();
					qp.getOperator().dump(queriesOutputFile);
					long elapsedMillis = System.currentTimeMillis() - startMillis;
					System.out.println("Number of milliseconds taken to evaluate query: " + elapsedMillis);

					// Check output for testing
					FileConverter.convertBinToHuman(queriesOutputFile, queriesOutputFile + "_humanreadable");
//					SortFile sort = new SortFile(queriesOutputFile + "_humanreadable", false);
//					sort.sortHuman();
//					SortFile sortExpected = new SortFile(
//							"/Users/jocelynsun/Desktop/CS 4321/cs4321/P1/expected_output" + File.separator + "query" + queryCount + "_humanreadable", false);
//					sortExpected.sortHuman();
					
					// Clean temp directory
					for (File tempFile : new File(tempDir).listFiles()) 
					    if (!tempFile.isDirectory()) tempFile.delete();
				} catch (Exception e) {
					System.err.println("Exception occurred during query " + queryCount);
					e.printStackTrace();
				}
				queryCount++;
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

}
