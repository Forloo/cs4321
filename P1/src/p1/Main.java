package p1;

import java.io.File;
import java.io.FileReader;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

public class Main {
	public static void main(String[] args) {
		String queriesFile = args[0] + File.separator + "queries.sql";
		String queriesOutput = args[1];
		
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			int queryCount = 1;
			while ((statement = parser.Statement()) != null) {
				try {
					System.out.println("Read statement: " + statement);
					Select select = (Select) statement;
					System.out.println("Select body is " + select.getSelectBody());
					
					File queryResult = new File(queriesOutput + File.separator + "query" + queryCount);
					queryResult.createNewFile();
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