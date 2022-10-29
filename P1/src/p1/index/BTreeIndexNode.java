package p1.index;

import java.util.ArrayList;
import java.util.Map;

public class BTreeIndexNode extends BTreeNode {
	// Min number of elements for each node unless root
	private int order;
	// Child nodes of the current node.
	private ArrayList<BTreeNode> childs;
	// The page the node is located on
	private int address;
	// The smallest value in this subtree 
	private int smallest;
	// The list of keys and the references they point to
	private ArrayList<Map.Entry<Integer, ArrayList<Integer>>> references;
	
	// Givens:
	// The 2d+1 nodes are given to us.
	// So we check the first node and 
	
	/**
	 * The constructor for a BTreeIndexNode
	 * @param order: The number of min elements that are in each node.
	 * @param childs: The children for this given node
	 */
	public BTreeIndexNode(int order, ArrayList<BTreeNode> childs, ArrayList<Map.Entry<Integer,ArrayList<Integer>>> references,int address) {
		this.order=order;
		this.childs=childs;
		this.references=references;
		this.address=address;
		int smallValue=Integer.MAX_VALUE;
		for(int i=0;i<this.childs.size();i++) {
			smallValue=Math.min(smallValue, childs.get(i).getSmallest());
		}
		this.smallest=smallValue;
	}

	/**
	 * returns the key value pairs for the index node
	 * @return references
	 */
	public ArrayList<Map.Entry<Integer, ArrayList<Integer>>> getReferences() {
		return this.references;
	}
	@Override
	/**
	 * Retrieves the order for this node.
	 */
	public int getOrder() {
		return this.order;
	}

	@Override
	/**
	 * Retrieves the size of the references. Tells us how many keys there are for each of the references
	 */
	public int getReferenceSize() {
		return this.references.size();
	}

	@Override
	/**
	 * Retrieves the smallest key for this subtree
	 */
	public int getSmallest() {
		return this.smallest;
	}
	
	/**
	 * Retrieves the address the index node is on.
	 * @return
	 */
	public int getAddress() {
		return this.address;
	}

	@Override
	/**
	 * Returns a list of the children for the given node.
	 */
	public ArrayList<BTreeNode> getChildren() {
		return this.childs;
	}
	
	

}
