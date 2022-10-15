package test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import p1.util.DatabaseCatalog;

class DatabaseCatalogTest {

	// Get the file list containing all file inputs
	File inputDir = new File("././input/db/data");
	String[] allFiles = inputDir.list();
	File[] fileList = new File[allFiles.length];
	// Schema file
	File schema = new File("././input/db/schema.txt");
	String tempDir = "././temp";
	File configFile = new File("././input/plan_builder_config.txt");

	@Test
	void constructorTesting() {
		// Initialize our file list
		for (int i = 0; i < allFiles.length; i++) {
			File file = new File(allFiles[i]);
			fileList[i] = file;
		}

		DatabaseCatalog information = DatabaseCatalog.getInstance(fileList, schema, configFile, tempDir);
		HashMap<String, String> names = information.getNames();

		// Expected absolute paths. Results will be different on other systems
		assertEquals("C:\\Users\\henry\\git\\cs4321\\P1\\Reserves", information.getNames().get("Reserves"));
		assertEquals("C:\\Users\\henry\\git\\cs4321\\P1\\Boats", information.getNames().get("Boats"));
		assertEquals("C:\\Users\\henry\\git\\cs4321\\P1\\Sailors", information.getNames().get("Sailors"));

		HashMap<String, ArrayList<String>> colInfo = information.getSchema();

		// Update the schema test as the columns now all have the prefix of the table that they are from
		ArrayList<String> reserveExpected = new ArrayList<String>();
		reserveExpected.add("Reserves.G");
		reserveExpected.add("Reserves.H");
		assertEquals(reserveExpected, colInfo.get("Reserves"));

		ArrayList<String> boatsExpected = new ArrayList<String>();
		boatsExpected.add("Boats.D");
		boatsExpected.add("Boats.E");
		boatsExpected.add("Boats.F");
		assertEquals(boatsExpected, colInfo.get("Boats"));

		ArrayList<String> sailorsExpected = new ArrayList<String>();
		sailorsExpected.add("Sailors.A");
		sailorsExpected.add("Sailors.B");
		sailorsExpected.add("Sailors.C");
		assertEquals(sailorsExpected, colInfo.get("Sailors"));

	}
}