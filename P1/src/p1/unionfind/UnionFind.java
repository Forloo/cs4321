package p1.unionfind;

import java.util.ArrayList;

public class UnionFind {

	private ArrayList<UnionFindElement> unionElements;

	/**
	 * Constructor for unionFind
	 */
	public UnionFind() {
		unionElements = new ArrayList<UnionFindElement>();
	}

	/**
	 * Find the UnionFindElement if the element is not in the UnionFind then make
	 * the element.
	 * 
	 * @param searchElement The UnionFind element that we are looking for.
	 * @return UnionFindElement containing the element.
	 */
	public UnionFindElement find(String searchElement) {
		for (int i = 0; i < unionElements.size(); i++) {
			if (unionElements.get(i).getAttributeSet().contains(searchElement)) {
				return unionElements.get(i);
			}
		}

		// Otherwise make the element for the given search key that we are given.
		// Add the element to the arraylist element to the given unionelement
		UnionFindElement newOne = new UnionFindElement();
		newOne.getAttributeSet().add(searchElement);
		return newOne;
	}

	/**
	 * Merge the two UnionFindElements together.
	 * 
	 * @param unionElementOne: The first unionFindElement to merge
	 * @param unionElementTwo: The second unionFindelement to merge.
	 * @return A new unionFindElement containing both elements.
	 */
	public UnionFindElement union(UnionFindElement unionElementOne, UnionFindElement unionElementTwo) {
		// Get the attribute set
		ArrayList<String> attrOne = unionElementOne.getAttributeSet();
		ArrayList<String> attrTwo = unionElementTwo.getAttributeSet();

		UnionFindElement curr = new UnionFindElement();
		ArrayList<String> attrThree = curr.getAttributeSet();

		// Loop through attrOne and then loop through attrTwo and if it contains either
		// of the two sets then add both of them.

		for (int i = 0; i < attrOne.size(); i++) {
			if (attrThree.contains(attrOne.get(i))) {
				continue;
			} else {
				attrThree.add(attrOne.get(i));
			}
		}

		for (int j = 0; j < attrTwo.size(); j++) {
			if (attrThree.contains(attrTwo.get(j))) {
				continue;
			} else {
				attrThree.add(attrTwo.get(j));
			}
		}
		// Update the min and max element
		int min_element = Math.max(unionElementOne.getMinValue(), unionElementTwo.getMinValue());
		int max_element = Math.min(unionElementOne.getMaxValue(), unionElementTwo.getMaxValue());

		curr.setMaxValue(max_element);
		curr.setMinValue(min_element);

		return curr;
	}

	// TODO
	public void setBounds(UnionFindElement unionElement, int lowerBound, int higherBound) {
		if (unionElement.getMaxValue() == null) {
			unionElement.setMaxValue(higherBound);
		} else {
			// Check the max element and if the value is more restrictive
			int max_element = Math.min(unionElement.getMaxValue(), higherBound);
		}

		if (unionElement.getMinValue() == null) {
			unionElement.setMinValue(lowerBound);
		} else {
			int min_element = Math.max(unionElement.getMinValue(), lowerBound);
		}

	}

	/**
	 * Merge two unionFind elements together into one UnionFind element
	 * 
	 * @param unionOne
	 * @param unionTwo
	 * @return
	 */
	public static UnionFind mergeUnions(UnionFind unionOne, UnionFind unionTwo) {
		// Get the first element from the first union element and then from the second
		// one get the list of that too

		// 1. Make a new unionfind to store the results
		// 2. Iterate through the first one and the second one order does not matter
		// 3. Check merge conditions and if there is overlap then make something for the
		// overlap
		// 4. Check if it is contained in our solution if not then add.
		
		if (unionOne.getUnionElement().size()==0) {
			return unionTwo;
		}
		else if(unionOne.getUnionElement().size()>0 && unionTwo.getUnionElement().size()==0) {
			return unionOne;
		}

		UnionFind curr = new UnionFind();
		for (int i = 0; i < unionOne.getUnionElement().size(); i++) {
			for (int j = 0; j < unionTwo.getUnionElement().size(); j++) {
				UnionFindElement first = unionOne.getUnionElement().get(i);
				UnionFindElement second = unionTwo.getUnionElement().get(j);
				// Check if there is overlap between the two elements
				boolean found = UnionFindElement.overlap(first, second);

				if (found) {
					UnionFindElement combined = curr.union(first, second);
					// Check if there is overlap with the UnionFind that we have
					int overlap = curr.containsAttribute(combined);
					if (overlap == -1) {
						curr.getUnionElement().add(combined);
					} else {
						UnionFindElement overlapping = curr.getUnionElement().get(overlap);
						UnionFindElement merged = curr.union(combined, overlapping);
						// We make a new element for merged so remove that element before adding the
						// interavl
						// otherwise the union will have more than one duplicate value
						curr.getUnionElement().remove(overlap);
						curr.getUnionElement().add(merged);
					}
				} else {
					// If there is no element in between them then that means they are disjoint and
					// tehn
					// we need to check if both of them are in the curr set.

					int overlap_first = curr.containsAttribute(first);

					if (overlap_first == -1) {
						// Not overlapping with the curr interval so just add it to the interval
						curr.getUnionElement().add(first);
					} else {
						UnionFindElement overlapping = curr.getUnionElement().get(overlap_first);
						UnionFindElement merged = curr.union(overlapping, first);
						curr.getUnionElement().remove(overlap_first);
						curr.getUnionElement().add(merged);
					}

					// If there no element in between the two then we need to add the column values
					// from
					// the second disjoint set into the result.
					int overlap_second = curr.containsAttribute(second);

					if (overlap_second == -1) {
						curr.getUnionElement().add(second);
					} else {
						UnionFindElement overlapping = curr.getUnionElement().get(overlap_second);
						UnionFindElement merged = curr.union(overlapping, second);
						curr.getUnionElement().remove(overlap_second);
						curr.getUnionElement().add(merged);
					}
				}
			}
		}

		return curr;
	}

	public ArrayList<UnionFindElement> getUnionElement() {
		return this.unionElements;
	}

	/**
	 * Check if there is overlapping elements in the set if we are to insert the
	 * current element in
	 * 
	 * @param element
	 * @return
	 */
	public int containsAttribute(UnionFindElement element) {

		ArrayList<UnionFindElement> curr = this.unionElements;

		for (int i = 0; i < curr.size(); i++) {
			UnionFindElement currElement = curr.get(i);
			if (UnionFindElement.overlap(currElement, element)) {
				return i;
			}
		}
		return -1;

	}

	/**
	 * Returns a string for testing.
	 */
	public String toString() {
		String ret = "";
		for (int i = 0; i < this.getUnionElement().size(); i++) {
			if (i == this.getUnionElement().size() - 1) {
				ret = ret + this.getUnionElement().get(i);
			} else {
				ret = ret + this.getUnionElement().get(i).toString() + "| ";
			}
		}
		return ret;
	}

	/**
	 * Returns a string to print with the logical plans.
	 */
	public String toStringFile() {
		String ret = "";
		for (int i = 0; i < this.getUnionElement().size(); i++) {
			ret = ret + this.getUnionElement().get(i).toStringFile();
		}
		return ret;
	}
}
