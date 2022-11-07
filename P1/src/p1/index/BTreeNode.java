package p1.index;

import java.util.ArrayList;
import java.util.Map;

public abstract class BTreeNode {
	
	// The number of keys per nodes or elements if clustered.
	public abstract int getOrder();
	// Number of keys if not clustered 
	public abstract int getReferenceSize();
	// Smallest value for the node. Not leaf node then smallest value in subtree.
	public abstract int getSmallest();
	// Address for the node
	public abstract int getAddress();
	// Children for the node
	public abstract ArrayList<BTreeNode> getChildren();
	// Returns the content of BTreeNode
	public abstract ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> getReference();
}
