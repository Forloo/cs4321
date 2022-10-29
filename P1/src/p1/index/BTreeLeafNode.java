package p1.index;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import p1.util.Tuple;

public class BTreeLeafNode extends BTreeNode{
	
	// The leaf node is either clustered or unclustered
	private boolean clustered;
	// The minimum number of nodes in the leaf node. There 
	private int order;
	// Key and mapping to the reference with that information null if clustered
	private ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> references=null;
	// Contains the tuple information if the index is clustered otherwise null
	private ArrayList<Tuple> information=null;
	// The smallest key in the leaf node for this page.
	private int smallestValue;
	// The page which the leaf node is on
	private int address;
	// The children for the leaf node
	private ArrayList<BTreeNode> childs;
	
	/**
	 * The constructor for the BTreeLeafNode
	 * @param clustered: Tells us if the leaf node contains references or the data itself.
	 * @param order: Tells us the number of keys the leaf node will have
	 * @param references: A hashmap of the references if the node is not clustered.
	 * @param information: An arraylist of the tuple values if the index is not clustered.
	 */
	public BTreeLeafNode(boolean clustered,int order, ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>> references,ArrayList<Tuple> information,int smallestValue,int address) {
		this.clustered=clustered;
		this.order=order;
		this.references=references;
		this.information=information;
		this.smallestValue=smallestValue;
		this.address=address;
		this.childs=null;
	}
	
	/**
	 * Retrieves all of the keys for the leaf node and the data entries for 
	 * @return
	 */
	public ArrayList<Map.Entry<Integer, ArrayList<TupleIdentifier>>>getReference(){
		return references;
	}
	
	/**
	 * Retrieves the tuples that are clustered in the leaf node
	 * @return An arraylist of tuples that are sorted on the chosen index column.
	 */
	public ArrayList<Tuple> getInformation(){
		return information;
	}
	
	/**
	 * Retrieves how many references the leaf node holds.
	 * @return
	 */
	public int getReferenceSize() {
		return references.size();
	}

	@Override
	/**
	 * Retrieves the order for the leaf node
	 */
	public int getOrder() {
		return this.order;
	}

	@Override
	public int getSmallest() {
		// TODO Auto-generated method stub
		return this.smallestValue;
	}
	
	/**
	 * Retrieves the address for the given node.
	 * @return An int representing the address for the leaf node.
	 */
	public int getAddress() {
		return this.address;
	}

	@Override
	/**
	 * Returns the children for the leaf node. Always null.
	 */
	public ArrayList<BTreeNode> getChildren() {
		return this.childs;
	}
	
	
	
	

}
