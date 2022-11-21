import bagel.Image;

/** This is the Fence class that handles all Fence initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Fence extends Actor {
    private final static Image img = new Image("res/images/fence.png");

    /** This is the constructor for the Femce class
     * Creates a Fence at given x and y co-ordinates.
     * @param x This is the initial x co-ordinate of the Fence.
     * @param y This is the initial y co-ordinate of the Fence.
     */
    public Fence(int x, int y) {
        super(x, y, img);
    }
}