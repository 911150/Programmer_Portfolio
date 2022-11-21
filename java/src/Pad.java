import bagel.Image;

/** This is the Pad class that handles all Pad initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Pad extends Actor {
    private final static Image img = new Image("res/images/pad.png");

    /** This is the constructor for the Pad class
     * Creates a Pad at given x and y co-ordinates.
     * @param x This is the initial x co-ordinate of the Pad.
     * @param y This is the initial y co-ordinate of the Pad.
     */
    public Pad(int x, int y) {
        super(x, y, img);
    }

}