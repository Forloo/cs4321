CS 4321 Project Phase 1
Contributors: Henry Chen (hc659), Jason Oh (jo293), Jocelyn Sun (js2997), Anna Zhang (az458)

The top level class of our code is in Main.java.


Logic for join conditions:
We broke the where condition down using our expressionparser class which breaks the expression down by doing recursion on the AND cases and then all other binary epxressions where the base case. Once the base case is reached we add it to a field to store all of the epxressions along with the tables needed for them.

The logic for our join conditions follow the description that is given on the handout. We parse each of the expressions on its own. For leaf nodes we check if there is a condition for that table. Then on non-leaf nodes we check if there is expression in a hashmap that includes both of the tables for that node if there are none then we do the crosspoduct otherwise we filter out tuples accordingly.
