import bagel.Image;

/** This is the Golden Tree class that handles all Golden Tree initialisation and implementation
 * @author Noah
 * @version 2
 */

public class GoldenTree extends Actor implements Harvestable {
    private final static Image img = new Image("res/images/gold-tree.png");

    /** This is the constructor for the Golden Tree class
     * Creates a Golden Tree at given x and y co-ordinates.
     * @param x This is the initial x co-ordinate of the Golden Tree.
     * @param y This is the initial y co-ordinate of the Golden Tree.
     */
    public GoldenTree(int x, int y) {
        super(x, y, img);
    }

    /**
     * This method does nothing since the tree has infinite fruit.
     */

    @Override
    public void harvest() {
        //do nothing
    }

    /**
     * This method returns true since the Golden Tree always has fruit.
      * @return Returns true.
     */
    @Override
    public boolean hasFruit() {
        return true;
    }
}