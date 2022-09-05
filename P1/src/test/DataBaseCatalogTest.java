package test;
import p1.databaseCatalog.*;

import static org.junit.jupiter.api.Assertions.fail;
import org.junit.*;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;


import org.junit.jupiter.api.Test;

class DataBaseCatalogTest{
	

	// Get the file list containing all file inputs
	File inputDir= new File("././input/db/data");
	String[] allFiles= inputDir.list();
	File [] fileList=new File[allFiles.length];
	// Schema file
	File schema= new File("././input/db/schema.txt");
	
	@Test 
	void constructorTesting() {
		// Initialize our file list
		for(int i=0;i<allFiles.length;i++) {
			File file = new File(allFiles[i]);
			fileList[i]=file;
		}
		
		DatabaseCatalog information = DatabaseCatalog.getInstance(fileList, schema);
		HashMap<String,String> names=information.getNames();
		
		// Expected absolute paths. Results will be different on other systems
		assertEquals("C:\\Users\\henry\\git\\cs4321\\P1\\Reserves",information.getNames().get("Reserves"));
		assertEquals("C:\\Users\\henry\\git\\cs4321\\P1\\Boats",information.getNames().get("Boats"));
		assertEquals("C:\\Users\\henry\\git\\cs4321\\P1\\Sailors",information.getNames().get("Sailors"));

		
		HashMap<String,ArrayList<String>> colInfo=information.getSchema();
		
		// Expected schemas
		ArrayList<String> reserveExpected = new ArrayList<String>();
		reserveExpected.add("G");
		reserveExpected.add("H");
		assertEquals(reserveExpected,colInfo.get("Reserves"));
		
		ArrayList<String> boatsExpected= new ArrayList<String>();
		boatsExpected.add("D");
		boatsExpected.add("E");
		boatsExpected.add("F");
		assertEquals(boatsExpected,colInfo.get("Boats"));
		
		ArrayList<String> sailorsExpected= new ArrayList<String>();
		sailorsExpected.add("A");
		sailorsExpected.add("B");
		sailorsExpected.add("C");
		assertEquals(sailorsExpected,colInfo.get("Sailors"));

		

	}
}