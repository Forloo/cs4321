package p1;

import java.io.File;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.io.FileConverter;
import p1.util.DatabaseCatalog;
import p1.util.LogicalPlan;
import p1.util.PhysicalPlanBuilder;
import p1.util.QueryPlan;

public class Main {

	public static void main(String[] args) {
		String queriesFile = args[0] + File.separator + "queries.sql";
		String queriesOutput = args[1];
		String dataDir = args[0] + File.separator + "db" + File.separator;
		String tempDir = args[2];

		// Get the file list containing all file inputs
		File inputDir = new File(dataDir + "data");
		String[] allFiles = inputDir.list();
		File[] fileList = new File[allFiles.length];
		File schema = new File(dataDir + "schema.txt");

		for (int i = 0; i < allFiles.length; i++) {
			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
			fileList[i] = file;
		}

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
//					SortFile sort = new SortFile(queriesOutputFile, true);
//					sort.sortHuman();
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
