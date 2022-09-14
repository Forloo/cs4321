package p1.operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Writer;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.QueryPlan;

public class JoinOperatorParser {
	
	PrintWriter writer;
//	= new PrintWriter("the-file-name.txt", "UTF-8");
	public JoinOperatorParser() {
		try{
            // Create new file
            String content = "This is the content to write into create file";
            String path="D:\\a\\hi.txt";
            File file = new File(path);

            // If file doesn't exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            // Write in file
            bw.write(content);

            // Close connection
            bw.close();
        }
        catch(Exception e){
            System.out.println(e);
        }
		
	}
	
	public Expression relevantExp(String tableName, Expression where) {
		//parse string and create file with relevant Expressions...
		String wString = where.toString(); //convert where to string 
		String[] splitWhere = wString.split("and"); //split the where string
		String[] tables = tableName.split(","); //get table names
		
		String finalExp = "";
		for (String indExp : splitWhere) {
			for (String table : tables) {
				if (indExp.contains(table) || indExp.contains(table)) {
					//if string after splitting contains string in tableName add to expression to evaluate concatenate using ands
					finalExp = finalExp + " and " + indExp;
				}
			}
		}
		//now that we have the expression in string format, convert that to text file and make expression object
		
		try{
            // Create new file
            String content = finalExp;
            String path="~/";
            File file = new File(path);

            // If file doesn't exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            // Write in file
            bw.write(content);

            // Close connection
            bw.close();
        }
        catch(Exception e){
            System.out.println(e);
        }
		
		
		
		
		
		
		
		
		
		
		
		JoinOperatorParser jop = new JoinOperatorParser(); //create jop that creates a file with query we want
		
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
					QueryPlan qp = new QueryPlan(statement, db);
					qp.getOperator().dump(queriesOutputFile);
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
		
		return where;
	}
	
	
}