package p1;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.index.BTree;
import p1.index.BTreeNode;
import p1.io.BPTreeWriter;
import p1.io.BinaryTupleWriter;
import p1.io.FileConverter;
import p1.operator.IndexScanOperator;
import p1.util.DatabaseCatalog;
import p1.util.LogicalPlan;
import p1.util.PhysicalPlanBuilder;
import p1.util.QueryPlan;
import p1.util.RandomDataGenerator;
import p1.util.StatGen;
import p1.util.Tuple;

public class Main {

	public static void main(String[] args) {
		File configFile = new File(args[0]);

		try {
			Scanner fileReader = new Scanner(configFile);
			String input = fileReader.nextLine();
			String queriesOutput = fileReader.nextLine();
			String tempDir = fileReader.nextLine() + File.separator;

			String queriesFile = input + File.separator + "queries.sql";
			String dataDir = input + File.separator + "db" + File.separator;

			String indexDir = dataDir + "indexes" + File.separator;

			fileReader.close();

			// Get the file list containing all file inputs
			File inputDir = new File(dataDir + "data");

			String[] allFiles = inputDir.list();

			File[] fileList = new File[allFiles.length];
			File schema = new File(dataDir + "schema.txt");

			for (int i = 0; i < allFiles.length; i++) {
				File file = new File(dataDir + "data" + File.separator + allFiles[i]);
				fileList[i] = file;
			}

			File indexInfo = new File(dataDir + "index_info.txt");
			DatabaseCatalog db = DatabaseCatalog.getInstance(fileList, schema, tempDir, indexInfo, indexDir);
			
			
			
			
			
			
			
//			generating Reserves File (added by Jason for testing)
//			String fileName3 = "/Users/jinseokoh/git/cs4321/P1/input/db/data/Pictures";
//			ArrayList<Tuple> rdg3 = new RandomDataGenerator(4,5000).generate();
//			BinaryTupleWriter writer3 = new BinaryTupleWriter(fileName3); 
//			for(Tuple t : rdg3) {
//			    writer3.writeTuple(t);
//			} writer3.close();
//			//for debugging
//			FileConverter.convertBinToHuman(fileName3, fileName3 + "_humanreadable");
			
			
			
			File statsFile = StatGen.generateStats(dataDir); // generates stats.txt
//			System.out.println(db.getStatsInfo());
//			for(String k : db.getStatsInfo().keySet()) {
//				System.out.println(k);
//				for(int i : db.getStatsInfo().get(k)) {
//					System.out.println(i);
//				}
//			}
			// this is how you use the new data generator
//			RandomDataGenerator rdg = new RandomDataGenerator(new File(dataDir+"Reserves")); // use generated file to generate random data
//			RandomDataGenerator rdg = new RandomDataGenerator(new File(dataDir + "stats.txt")); // use custom file to generate random data
//			rdg.generateAll(dataDir + "data" + File.separator);

			for (String key : db.getIndexInfo().keySet()) { // generate all indexes specified
				String[] idxInfo = db.getIndexInfo().get(key);
				File indexFileLocation = new File(indexDir + File.separator + key);
				boolean clus = idxInfo[0].equals("1"); // true if clustered index
				String tableName = key.split("\\.")[0];
				String tablePath = db.getNames().get(tableName);

				int order = Integer.valueOf(idxInfo[1]);
				int colIdx = DatabaseCatalog.getInstance().getSchema().get(tableName).indexOf(key);
				BTree bTree = new BTree(order, clus, colIdx, indexFileLocation, tablePath, 0, tableName, tempDir);
				BTreeNode root = bTree.constructTree();
				bTree.setRoot(root);
				BPTreeWriter bptw = new BPTreeWriter(bTree.getAllLevels(), indexFileLocation, bTree.getRoot(), order);

					String path= "/Users/annazhang/db/cs4321/P1/input/db/indexes/Boats.E";
					String sailorsPath="/Users/annazhang/db/cs4321/P1/input/db/indexes/Sailors.A";
//					System.out.println(tableName);
//					BPTreeReader tr = new BPTreeReader(sailorsPath);
					IndexScanOperator scan= new IndexScanOperator(tableName, null, 1000, clus, colIdx, sailorsPath);
//					System.out.println(tableName);
//					scan.dump();
//					System.out.println("=====================================");
//					scan.reset();
//					System.out.println("Reset method is tested here");

			}

			try {
				CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
				Statement statement;
				int queryCount = 1;

				while ((statement = parser.Statement()) != null) {
					try {
						// Parse statement
						Select select = (Select) statement;
						PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

						// Create results file in output file directory
						String queriesOutputFile = queriesOutput + File.separator + "query" + queryCount;
						File queryResult = new File(queriesOutputFile);
						queryResult.createNewFile();

						// Evaluate query
						LogicalPlan lp = new LogicalPlan(statement);
						System.out.println(statement);
						PhysicalPlanBuilder builder = new PhysicalPlanBuilder(statement);
						lp.accept(builder,db.statsInfo);
						QueryPlan qp = builder.getPlan();
						long startMillis = System.currentTimeMillis();
						qp.getOperator().dump(queriesOutputFile);
						long elapsedMillis = System.currentTimeMillis() - startMillis;
						System.out.println("Number of milliseconds taken to evaluate query: " + elapsedMillis);
						System.out.println("Finished the current query");
						// Generate logical/physical plan files
						String logicalOutputFile = queriesOutputFile + "_logicalplan";
						String physicalOutputFile = queriesOutputFile + "_physicalplan";
						try {
							// Logical plan
//							System.out.println("generating logical plan"); //PRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINT
							File logFile = new File(logicalOutputFile);
							Path logFilePath = Paths.get(logicalOutputFile);
							ArrayList<String> logPlan = new ArrayList<String>();
							logPlan.add(lp.getOperator().toString(0));
//							System.out.println("is this the logical plan?"+lp.getOperator().toString(0)); //PRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINT
							
							Files.write(logFilePath, logPlan, StandardCharsets.UTF_8);
//							System.out.println("finished making logical plan"); //PRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINTPRINT
							// Physical plan
							File physFile = new File(physicalOutputFile);
							Path physFilePath = Paths.get(physicalOutputFile);
							ArrayList<String> physPlan = new ArrayList<String>();
							physPlan.add(qp.getOperator().toString(0));
//							System.out.println(qp.getOperator().toString(0));
							// Im guessing the physPlan only get the first operator and that is
							// why there is nothing in the tree right now.
							Files.write(physFilePath, physPlan, StandardCharsets.UTF_8);
						} catch (IOException e) {
							System.out.println("Error writing plan file: ");
							e.printStackTrace();
						}

						// Check output for testing
						FileConverter.convertBinToHuman(queriesOutputFile, queriesOutputFile + "_humanreadable");
//					SortFile sort = new SortFile(queriesOutputFile + "_humanreadable", false);
//					sort.sortHuman();
//					SortFile sortExpected = new SortFile(
//							"/Users/jocelynsun/Desktop/CS 4321/cs4321/P1/expected_output" + File.separator + "query" + queryCount + "_humanreadable", false);
//					sortExpected.sortHuman();

						// Clean temp directory
						for (File tempFile : new File(tempDir).listFiles())
							if (!tempFile.isDirectory())
								tempFile.delete();
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
		} catch (Exception e) {
			System.err.println("Exception occurred during config file parsing");
			e.printStackTrace();
		}
	}

	private static void testing(String tempDir) {
//// ===================================== testing B tree writer =================================
//		File file = new File("/Users/annazhang/git/cs4321/P1/temp");
//		BTree testingOne = new BTree(15,false,0,file,"/Users/annazhang/db/cs4321/P1/input/db/data/Sailors",0, "Sailors", tempDir);
//		BTreeNode root = testingOne.constructTree();
//		System.out.println(testingOne.getAllLevels()); 
//		testingOne.setRoot(root);
//		System.out.println(testingOne.getRoot());
//		BPTreeWriter bptw = new BPTreeWriter(testingOne.getAllLevels(), file, testingOne.getRoot(),15);
//		
//		BPTreeReader btr = new BPTreeReader("/Users/annazhang/git/cs4321/P1/temp");
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
//		System.out.println(v);
//		while(v == true) {//reach index nodes
//			c += 1;
//			v = btr.checkNodeType();
//		}
//		btr.checkNodeType();
//		int btrKey= btr.getNextKey();
//		System.out.println(btrKey);
//		while(( btrKey  != -1)) {
//			System.out.println(btrKey); //print the keys in the first indx node
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
//// ===================================== testing B tree writer =================================
//
//// ============================= debugging by generating random data ===========================
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
// ============================= debugging by generating random data ===========================
//	}

	}
}
