package p1.io;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import p1.util.Tuple;

/**
 * A tuple reader that reads text file input.
 */
public class HumanTupleReader implements TupleReader {

	// The path of the human-readable file to read from.
	private String file;
	// Scanner that goes through lines of the file.
	private Scanner fileReader;

	/**
	 * Creates a Scanner that reads from the input file.
	 *
	 * @param file the human-readable file to read from
	 * @throws IOException
	 */
	public HumanTupleReader(String filePath) {
		try {
			file = filePath;
			fileReader = new Scanner(new File(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read the next tuple.
	 *
	 * @return the next tuple.
	 * @throws IOException
	 */
	@Override
	public Tuple nextTuple() {
		// Reset buffer when it has read an entire page.
		if (fileReader.hasNextLine()) {
			return new Tuple(fileReader.nextLine());
		}
		close();
		return null;
	}

	/**
	 * Closes the reader.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() {
		fileReader.close();
	}

	/**
	 * Resets the reader.
	 *
	 * @throws IOException
	 */
	@Override
	public void reset() {
		try {
			if (fileReader != null) {
				close();
			}
			fileReader = new Scanner(new File(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Resets the reader to the ith tuple.
	 */
	public void reset(int idx) {
		try {
			if (fileReader != null) {
				close();
			}
			fileReader = new Scanner(new File(file));
			for (int i = 0; i < idx; i++) {
				nextTuple();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
