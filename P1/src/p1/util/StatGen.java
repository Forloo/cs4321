package p1.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import p1.io.BinaryTupleReader;

public class StatGen {
	/**
	 * Generates a stats file for all tables.
	 * 
	 * @param fileLoc the file directory for the tables
	 * @return the stats file
	 */
	public static File generateStats(String fileLoc) {
		// Get locations of binary data files
		File inputDir = new File(fileLoc + "data");
		String[] allFiles = inputDir.list();

		// Generate new stats.txt file
		File statsFile = new File(fileLoc + "stats.txt");
		ArrayList<String> lines = new ArrayList<String>();
		Path file = Paths.get(fileLoc + "stats.txt");

		// Iterate through each relation
		for (int i = 0; i < allFiles.length; i++) {
			String line = ""; // Line representing the table info
			String relation = allFiles[i];
			if (!relation.contains("humanreadable")) { // Only generate stats for binary data files
				line += relation + " ";
				int numTuples = 0;
				ArrayList<String[]> attr = new ArrayList<String[]>(); // {[col1, min, max], [col2, min, max], ...}

				BinaryTupleReader bfr = new BinaryTupleReader(fileLoc + "data" + File.separator + relation);
				Tuple t = bfr.nextTuple();

				// Store attribute info
				ArrayList<String> cols = DatabaseCatalog.getInstance().getSchema().get(relation);
				for (int j = 0; j < cols.size(); j++) {
					String col = cols.get(j).substring(cols.get(j).lastIndexOf(".") + 1);
					attr.add(
							new String[] { col, String.valueOf(Integer.MAX_VALUE), String.valueOf(Integer.MIN_VALUE) });
				}

				// Iterate through each tuple
				while (t != null) {
					numTuples++;
					// Iterate through each attribute for each tuple
					for (int j = 0; j < cols.size(); j++) {
						// Check min value of attribute
						if (Integer.parseInt(t.getTuple().get(j)) < Integer.parseInt(attr.get(j)[1])) {
							attr.set(j, new String[] { attr.get(j)[0], t.getTuple().get(j), attr.get(j)[2] });
						}
						// Check max value of attribute
						if (Integer.parseInt(t.getTuple().get(j)) > Integer.parseInt(attr.get(j)[2])) {
							attr.set(j, new String[] { attr.get(j)[0], attr.get(j)[1], t.getTuple().get(j) });
						}
					}
					t = bfr.nextTuple();
				}

				line += numTuples + " ";
				DatabaseCatalog.getInstance().statsInfo.put(relation, new int[] { numTuples });
				for (String[] info : attr) {
					line += String.join(",", info) + " ";
					DatabaseCatalog.getInstance().statsInfo.put(relation + "." + info[0],
							new int[] { Integer.parseInt(info[1]), Integer.parseInt(info[2]) });
				}

				lines.add(line.substring(0, line.length() - 1)); // Don't want last space in line
			}
		}

		try {
			Files.write(file, lines, StandardCharsets.UTF_8);
		} catch (IOException e) {
			System.out.println("Error writing stats file: ");
			e.printStackTrace();
		}

		return statsFile;
	}
}
