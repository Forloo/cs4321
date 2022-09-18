package p1.io;

import java.io.IOException;

import p1.util.Tuple;

/**
 * Converts a binary file to a human-readable file and vice versa.
 */
public class FileConverter {

	/**
	 * Reads lines from a binary file and writes those lines to a huamn-readable
	 * file.
	 *
	 * @param binPath   the binary file path.
	 * @param humanPath the human-readable file path.
	 * @throws IOException
	 */
	public static void convertBinToHuman(String binPath, String humanPath) {
		BinaryTupleReader reader = new BinaryTupleReader(binPath);
		HumanTupleWriter writer = new HumanTupleWriter(humanPath);
		Tuple row = reader.nextTuple();
		while (row != null) {
			writer.writeTuple(row);
			row = reader.nextTuple();
		}
		reader.close();
		writer.close();
	}

	/**
	 * Reads lines from a huamn-readable file and writes those lines to a binary
	 * file.
	 *
	 * @param humanPath the huamn-readable file path.
	 * @param binPath   the binary file path.
	 * @throws IOException
	 */
	public static void convertHumanToBin(String humanPath, String binPath) {
		HumanTupleReader reader = new HumanTupleReader(humanPath);
		BinaryTupleWriter writer = new BinaryTupleWriter(binPath);
		Tuple row = reader.nextTuple();
		while (row != null) {
			writer.writeTuple(row);
			row = reader.nextTuple();
		}
		reader.close();
		writer.close();
	}

}
