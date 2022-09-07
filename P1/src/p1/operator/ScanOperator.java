package p1.operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;

import p1.Tuple;
import p1.databaseCatalog.DatabaseCatalog;

/**
 * An operator that opens a file scan on the appropriate data file.
 */
public class ScanOperator extends Operator {

	// rows of the table
	private ArrayList<String> rows;
	// index of next item/tuple
	private int idx;

	/**
	 * Constructor to scan rows of table fromTable.
	 */
	public ScanOperator(String fromTable) {
		rows = new ArrayList<String>();
		idx = 0;
		try {
			String fileLoc = DatabaseCatalog.getInstance().getNames().get(fromTable);
			File file = new File(fileLoc);
			Scanner fileReader = new Scanner(file);
			while (fileReader.hasNextLine()) {
				rows.add(fileReader.nextLine());
			}
			fileReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	/**
	 * Retrieves the next tuples. If there is no next tuple then null is returned.
	 *
	 * @return the tuples representing rows in a database
	 */
	public Tuple getNextTuple() {
		if (idx == rows.size()) {
			return null;
		}
		return new Tuple(rows.get(idx++));
	}

	/**
	 * Tells the operator to reset its state and start returning its output again
	 * from the beginning
	 */
	public void reset() {
		idx = 0;
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to System.out.
	 */
	public void dump() {
		while (idx < rows.size()) {
			System.out.println(getNextTuple().toString());
		}
	}

	/**
	 * This method repeatedly calls getNextTuple() until the next tuple is null (no
	 * more output) and writes each tuple to a new file.
	 *
	 * @param outputFile the file to write the tuples to
	 */
	public void dump(String outputFile) {
		try {
			PrintWriter out = new PrintWriter(outputFile);

			while (idx < rows.size()) {
				out.println(getNextTuple().toString());
			}

			out.close();
		} catch (Exception e) {
			System.out.println("Exception occurred: ");
			e.printStackTrace();
		}
	}
	
}
