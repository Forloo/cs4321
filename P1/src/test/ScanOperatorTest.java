//package test;
//
//import static org.junit.Assert.assertNull;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//
//import java.io.File;
//
//import org.junit.jupiter.api.Test;
//
//import p1.operator.ScanOperator;
//import p1.util.DatabaseCatalog;
//
//class ScanOperatorTest {
//
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
//	void testGetNextTuple() {
//		for (int i = 0; i < allFiles.length; i++) {
//			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
//			fileList[i] = file;
//		}
//
//		DatabaseCatalog.getInstance(fileList, schema, configFile, tempDir);
//		ScanOperator so = new ScanOperator("Sailors");
//
//		assertEquals("1,200,50", so.getNextTuple().toString());
//		assertEquals("2,200,200", so.getNextTuple().toString());
//		assertEquals("3,100,105", so.getNextTuple().toString());
//		assertEquals("4,100,50", so.getNextTuple().toString());
//		assertEquals("5,100,500", so.getNextTuple().toString());
//		assertEquals("6,300,400", so.getNextTuple().toString());
//		assertNull(so.getNextTuple());
//		assertNull(so.getNextTuple());
//	}
//
//	@Test
//	void testReset() {
//		for (int i = 0; i < allFiles.length; i++) {
//			File file = new File(dataDir + "data" + File.separator + allFiles[i]);
//			fileList[i] = file;
//		}
//
//		DatabaseCatalog.getInstance(fileList, schema, configFile, tempDir);
//		ScanOperator so = new ScanOperator("Sailors");
//
//		so.getNextTuple();
//		so.getNextTuple();
//		so.getNextTuple();
//		so.reset();
//
//		assertEquals("1,200,50", so.getNextTuple().toString());
//	}
//
//}
