CS 4321 Project Phase 2 Contributors: Henry Chen (hc659), Jason Oh (jo293), Jocelyn Sun (js2997), Anna Zhang (az458)

The top level class of our code is in Main.java.

Logic for join conditions: We broke the where condition down using our expressionparser class which breaks the expression down by doing recursion on the AND cases and then all other binary epxressions where the base case. Once the base case is reached we add it to a field to store all of the epxressions along with the tables needed for them.

The logic for our join conditions follow the description that is given on the handout. We parse each of the expressions on its own. For leaf nodes we check if there is a condition for that table. Then on non-leaf nodes we check if there is expression in a hashmap that includes both of the tables for that node if there are none then we do the crosspoduct otherwise we filter out tuples accordingly.

The logical operators are in package p1.logicaloperator. The physical operators are in package p1.operator. The PhysicalPlanBuilder is in p1.util.

The partition reset was handled in SMJ using two pointers and a reset(int idx) method as suggested in the project instructions. One pointer keeps track of the index of the start of the current partition, while the other pointer keeps track of the current index of the tuple from the inner table we are looking at. We kept DISTINCT the same as project 1, where we only kept track of the previous tuple returned by calling childOperator.getNextTuple(). We then compare the current tuple with this previous tuple and check if they are the same, and if not, we call getNextTuple() again. This is not unbounded because the memory complexity is the same every time (we only need one object to keep track of the previous tuple), except of course when we specify in the configuration file that we want to use the in-memory sort. Similarly, in SMJ, we do not use a data structure to store lists of tuples and instead only store one current tuple from the outer table and one current tuple for the inner table, allowing us to keep memory constant.

Logic for index scan operator: The lowkey and highkey are set in PhysicalPlanBuilder in the util package. The clustered and unclustered indexes are built differently in BTree in the index package, and they are handled differently in getNextTuple in IndexScanOperator in the operator package.

Logic for the root-to-leaf descent: Using the index from the header page we navigate our binary reader to the header page. With the header page information we find the address of the of the root page. Using the index pages then we use our low key value. Then we find the first key value that is lower than the low key value and go that given address. If is not lower than any of the keys then the last key has a pointer going to values higher than that last key value. Continue doing this until we reach a leaf node. Then given the address of that smallest leaf node page we keep going to the next leaf node page until the conditions are violated if we reach an index page meaning that there are no leaf pages left.

In the physical plan builder, the selection was handled by parsing the WHERE condition, splitting up each condition on the string " AND ", and comparing the integers with the current "high" and current "low" values.

TODO: For each of the below algorithms/functionalities, an explanation of where the implementation is found (i.e. which classes/methods perform it), as well as an explanation of your logic, especially if your logic diverges in any way from the instructions. If your logic is adequately explained in comments in your code, you may provide a reference to the comment rather than copying the comment:

TODO:
– the choice of implementation for each logical selection operator
– the choice of the join order

Explanation for selection pushing: Using the unionfind element we group all of the attributes that have the same conditions together. Using the visitor class that we used to parse all of the expression depending on the expression type we make a specific unionfind element and if that element exist already then join them into one larger unionfind element. In the logical plan then for a given table we check if it has unionfind conditions and conditions that are not handled by the unionfind and add them appropriately. Then using the physicalplanbuilder we make the physical plan and the physical operators with all the conditions from the logical plan operators and the restraints from the unionfind.

Choice of implementation for each join operator: In Project 2 benchmarking, we noticed that SMJ and BNLJ ran similarly, with SMJ much faster if the number of output tuples was low, so we implemented all joins as SMJ where possible. However, SMJ does not apply to joins that have other-than-equality comparisons or to pure cross-products, so those are implemented using BNLJ.

Explanation for join order: The implementation can be found under the P1/src/p1/dp. The constructor initializes the v-value array used to calculate the intermediate join cost. v-value for base table is simply max-min+1, v-value for selection is the minimum v-value among the attribute involved in selection (attribute's v-value calculated by multiplying the relation's number of tuple with the reduction factor), and the v-value for output of a join is the minimum v-value of attributes involved in the join condition. The reduction factor for selection is simply (number of attributes in selection condition) / (attribute max - attribute min). Unclear reduction factors mentioned in the handout is not considered. At the end, the v-values are either rounded to 1 if too small or rounded down to never be greater than the estimate of join size. Using this v-value array, a dictionary for the intermediate join cost is initialized. Every possible pair gets formed and gets stored as the key and the value its join cost. From this initialized values, the dynammic programming algorithm is used to calculate the join order with min cost.
