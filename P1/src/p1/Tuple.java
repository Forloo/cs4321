package p1;

public class Tuple {

	String[] row;
	
	public Tuple(String rowStr) {
		this.row = rowStr.split(",");
	}
	
}
