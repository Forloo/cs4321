package test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileReader;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import p1.util.ExpressionEvaluator;
import p1.util.Tuple;

class ExpressionEvaluatorTest {

	ArrayList<String> schema = new ArrayList<String>();

	@Test
	void testEvaluator() {
		schema.add("A");
		schema.add("B");
		schema.add("C");

		ExpressionEvaluator evaluator = new ExpressionEvaluator(new Tuple("10,20,30"), schema);

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader("src/test/queries.sql"));
			Statement statement;
			int idx = 0;
			while ((statement = parser.Statement()) != null) {
				Select select = (Select) statement;
				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

				plainSelect.getWhere().accept(evaluator);
				String expected = idx % 2 == 0 ? "true" : "false";
				assertEquals(expected, evaluator.getValue());
				idx++;
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

}
