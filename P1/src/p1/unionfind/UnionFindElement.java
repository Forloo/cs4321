package p1.unionfind;

import java.util.ArrayList;

/**
 * A class representing a single element in our Union-find data structure.
 * 
 * @author Henry Chen
 *
 */
public class UnionFindElement {

	// What does each unionfindelement need to have
	// 1. There is always a lower bound
	// 2. There is an upper bound value
	// 3. We need something that tells us the attributes that are in the current
	// union find element
	// 4. We need something to tell us if the condition is an equality meaning that
	// everything is constrained.
	// to bet the same value or some specified value that is given to us.

	// The attributes for the current union find element.
	private ArrayList<String> attributeSet;
	// The min element in the unionfind set
	private Integer minValue;
	// The max element in the unionfind set
	private Integer maxValue;
	// Whether the element is in an equality constraint or not
	private Boolean equality;

	public UnionFindElement() {
		this.minValue = Integer.MIN_VALUE;
		this.maxValue = Integer.MAX_VALUE;
		this.equality = null;
		this.attributeSet = new ArrayList<String>();
	}

	/**
	 * Retrieves the min value for UnionFindElement
	 * 
	 * @return int: representing the min value for UnionFindElement.
	 */
	public Integer getMinValue() {
		return this.minValue;
	}

	/**
	 * Sets the min value for the unionFindElement
	 * 
	 * @param minValue : The new minValue for the UnionFindElement.
	 */
	public void setMinValue(int minValue) {
		this.minValue = minValue;
	}

	/**
	 * Retrieves the max value for the unionFindElement.
	 * 
	 * @return int: representing the max value for the UnionFindElement.
	 */
	public Integer getMaxValue() {
		return this.maxValue;
	}

	/**
	 * Sets the max value for the unionFindElement.
	 * 
	 * @param maxValue : The new maxValue for the UnionFindElement.
	 */
	public void setMaxValue(int maxValue) {
		this.maxValue = maxValue;
	}

	public void setEquality(Boolean equalityValue) {
		this.equality = equalityValue;
	}
	private String toStringEqualityHelper(int input,Boolean equality) {
		if (equality==null) {
			return "null";
		}
		else if (equality==false) {
			return "null";
		}

		// If the value of the max value is the max value then it cannot be an 
		// inequality operation since that is not in the range of the valid values

		if (input==Integer.MAX_VALUE) {
			return "null";
		}

		return Integer.toString(input);
	}
	/**
	 * Retrieves the attribute set for the union find element.
	 * 
	 * @return An arraylist of string representing the attributes in the set.
	 */
	public ArrayList<String> getAttributeSet() {
		return this.attributeSet;
	}
	
	private String toStringHelper(int input, boolean isMin) {
//		System.out.println("The string helper method was called by our function");
		if (isMin && input==Integer.MIN_VALUE) {
			return "null";
		}
		else if(isMin && input!=Integer.MIN_VALUE){
			return Integer.toString(input);
		}
		else if (!isMin && input==Integer.MAX_VALUE) {
			return "null";
		}

		return Integer.toString(input);
	}
	
	public static boolean overlap(UnionFindElement first, UnionFindElement second) {
		ArrayList<String> firstAttr = first.getAttributeSet();
		ArrayList<String> secondAtr = second.getAttributeSet();

		for (int i = 0; i < firstAttr.size(); i++) {
			String curr = firstAttr.get(i);
			if (secondAtr.contains(curr)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns string for testing
	 */
	public String toString() {
		String ret = "";
		ret = ret + "Attributes:";
		for (int i = 0; i < this.getAttributeSet().size(); i++) {
			String curr = this.getAttributeSet().get(i);
			if (i == 0) {
				ret = ret + curr;
			} else {
				ret = ret + ", " + curr;
			}
		}

		ret = ret + " " + "Min-value" + this.getMinValue();
		ret = ret + " " + "Max-Value" + this.getMaxValue();
		ret = ret + " " + "IsEquality" + equality;
		return ret;
	}

	/**
	 * Returns string for writing to the logical plan file.
	 */
	public String toStringFile() {
		String ret = "[[";
		ret += String.join(", ", this.getAttributeSet()) + "], ";

//		ret += "equals " + equality + ", ";
//		ret += "min " + this.getMinValue() + ", ";
//		ret += "max " + this.getMaxValue();
		ret += "equals " + this.toStringEqualityHelper(this.getMaxValue(), equality) + ", ";
		ret += "min " + this.toStringHelper(this.getMinValue(), true) + ", ";
		ret += "max " + this.toStringHelper(this.getMaxValue(),false);
		return ret + "]\n";
	}
}
