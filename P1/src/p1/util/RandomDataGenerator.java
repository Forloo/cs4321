package p1.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

import p1.io.BinaryTupleWriter;
import p1.io.FileConverter;

/**
 * Generates a specified number of random tuples of the same size.
 */
public class RandomDataGenerator {

	// The randomly generated tuples.
	private ArrayList<Tuple> tuples;
	// The number of columns/attributes.
	private int numCol;
	// The number of rows/tuples.
	private int numRow;
	// table stats made by StatGen
	// key: table name with num tuples as the value.
	private HashMap<String, Integer> tableInfo;
	// key is table name + column name, and value is 2-element array with arr[0]
	// = low, arr[1] = high. example: {Sailors: [10000], Sailors.A: [0, 10000],
	// Sailors.B: [0, 100], Boats: [1000], Boats.D: ...}
	private HashMap<String, int[]> statsInfo;
	private HashMap<String, ArrayList<String>> schema;

	/**
	 * Sets the number of attributes and rows for the tuple list.
	 *
	 * @param numAttr   the number of columns.
	 * @param numTuples the number of tuples.
	 */
	public RandomDataGenerator(int numAttr, int numTuples) {
		numCol = numAttr;
		numRow = numTuples;
	}

	/**
	 * Sets the number of attributes and rows for the tuple list using a stats file.
	 *
	 * @param statsFile the file with data info
	 */
	public RandomDataGenerator(File statsFile) {
		try {
			statsInfo = new HashMap<String, int[]>();
			tableInfo = new HashMap<String, Integer>();
			schema = new HashMap<String, ArrayList<String>>();
			Scanner fileReader = new Scanner(statsFile);
			while (fileReader.hasNextLine()) {
				ArrayList<String> cols = new ArrayList<String>();
				String info = fileReader.nextLine();
				String[] tableStats = info.split(" ");
				String tableName = tableStats[0];
				tableInfo.put(tableName, Integer.parseInt(tableStats[1]));
				for (int i = 2; i < tableStats.length; i++) {
					String[] colStats = tableStats[i].split(",");
					statsInfo.put(tableName + "." + colStats[0],
							new int[] { Integer.parseInt(colStats[1]), Integer.parseInt(colStats[2]) });
					cols.add(colStats[0]);
				}
				schema.put(tableName, cols);
			}
			fileReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("Error generating random data: ");
			e.printStackTrace();
		}
	}

	/**
	 * Generates the specified number of tuples.
	 *
	 * @return an ArrayList of Tuples.
	 */
	public ArrayList<Tuple> generate() {
		tuples = new ArrayList<Tuple>();
		Random rand = new Random();
		for (int i = 0; i < numRow; i++) {
			String tuple = "";
			for (int j = 0; j < numCol - 1; j++) {
				tuple += rand.nextInt(1000) + ",";
			}
			tuples.add(new Tuple(tuple + rand.nextInt(1000)));
		}
		return tuples;
	}

	/**
	 * Generates the specified data in the stats file (can be used to generate
	 * multiple files at once).
	 */
	public void generateAll(String dataDir) {
		for (String key : tableInfo.keySet()) { // for every table
			BinaryTupleWriter writer = new BinaryTupleWriter(dataDir + key);
			Random rand = new Random();
			for (int i = 0; i < tableInfo.get(key); i++) { // num tuples
				String tuple = "";
				for (String col : schema.get(key)) { // for every column
					int max = statsInfo.get(key + "." + col)[1];
					int min = statsInfo.get(key + "." + col)[0];
					tuple += rand.nextInt(max - min) + min + ",";
				}
				writer.writeTuple(new Tuple(tuple.substring(0, tuple.length() - 1)));
			}
			writer.close();
			FileConverter.convertBinToHuman(dataDir + key, dataDir + key + "_humanreadable");
		}
	}
}
