package p1;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.io.FileConverter;
import p1.util.DatabaseCatalog;
import p1.util.QueryPlan;

public class Main {

	public static void main(String[] args) {
		String queriesFile = args[0] + File.separator + "queries.sql";
		String queriesOutput = args[1];
		String dataDir = args[0] + File.separator + "db" + File.separator;
		String tempDir = args[2];

		// Get the file list containing all file inputs
		File inputDir = new File(dataDir + "data");
		System.out.println(args[0] + "/data");
		String[] allFiles = inputDir.list();
		File[] fileList = new File[allFiles.length];
		File schema = new File(dataDir + "schema.txt");

		for (int i = 0; i < allFiles.length; i++) {
			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
			fileList[i] = file;
		}

		DatabaseCatalog db = DatabaseCatalog.getInstance(fileList, schema);

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			int queryCount = 1;

			// =============================== TEST IO ===============================
//			BinaryTupleWriter btw = new BinaryTupleWriter(queriesOutput + File.separator + "testbin");
//			RandomDataGenerator rdg = new RandomDataGenerator(3, 10000);
//			PrintWriter out = new PrintWriter(queriesOutput + File.separator + "testnormal");
//			for (Tuple t : rdg.generate()) {
//				out.println(t.toString());
//				btw.writeTuple(t);
//			}
//			out.close();
//			btw.close();
//			BinaryTupleReader btr = new BinaryTupleReader(queriesOutput + File.separator + "testbin");
//			for (int i = 0; i < 10000; i++) {
//				System.out.println(btr.nextTuple().toString());
//			}
//			btr.close();
			// =============================== TEST IO ===============================

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
					QueryPlan qp = new QueryPlan(statement, db);
					long startMillis = System.currentTimeMillis();
					qp.getOperator().dump(queriesOutputFile);
					long elapsedMillis = System.currentTimeMillis() - startMillis;
					System.out.println("Number of milliseconds taken to evaluate query: " + elapsedMillis);

					// Check output for testing
					FileConverter.convertBinToHuman(queriesOutputFile, queriesOutputFile + "_humanreadable");
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
