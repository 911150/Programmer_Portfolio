import bagel.Image;

/** This is the Mitosis Pool class that handles all Mitosis Pool initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Pool extends Actor {
    private final static Image img = new Image("res/images/pool.png");

    /** This is the constructor for the Mitosis Pool class
     * Creates a Mitosis Pool at given x and y co-ordinates.
     * @param x This is the initial x co-ordinate of the Mitosis Pool.
     * @param y This is the initial y co-ordinate of the Mitosis Pool.
     */

    public Pool(int x, int y) {
        super(x, y, img);
    }

}