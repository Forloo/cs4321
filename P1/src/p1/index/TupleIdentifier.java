package p1.index;

public class TupleIdentifier {
	
	// The page that the tuple is on
	private int pageId;
	// The tuple number for which the tuple is located on the given page
	private int tupleId;
	
	/**
	 * The constructor for the tupleIdentifier class which tells us the page and 
	 * which tuple the current tuple is on.
	 * @param pageId: The page the tuple is on
	 * @param tupleId: The number of tuple on that page.
	 */
	public TupleIdentifier(int pageId,int tupleId) {
		this.pageId=pageId;
		this.tupleId=tupleId;
	}
	
	/**
	 * Retrieves the page for the current tuple that we are trying to retrieve
	 * @return An int giving us which page the tuple is on
	 */
	public int getPageId() {
		return this.pageId;
	}
	
	/**
	 * Retrieves the tupleid for the current tuple that we are trying to retrieve.
	 * @return An int giving us the ith number for which the tuple is on this page.
	 */
	public int getTupleId() {
		return this.tupleId;
	}
	
	/**
	 * A method that will represent the tupleIdentifier as a string object in terms
	 * of the page it is located on and the number of the tuple it is on that page.
	 */
	public String toString() {
		return Integer.toString(pageId)+Integer.toString(tupleId);
	}
}
