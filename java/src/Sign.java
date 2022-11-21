import bagel.Image;
import bagel.util.Vector2;

/** This is the Sign class that handles all Sign initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Sign extends Actor {
    private final Vector2 direction;

    /** This is the constructor for the Sign class
     * Creates a Sign at given x and y co-ordinates with the given direction.
     * @param x This is the initial x co-ordinate of the Sign.
     * @param y This is the initial y co-ordinate of the Sign.
     * @param img This is the image of the Sign.
     * @param direction This is the direction of the Sign as a Vector2.
     */
    public Sign(int x, int y, Image img, Vector2 direction) {
        super(x, y, img);
        this.direction = direction;
    }

    /**
     * This method returns the direction that the Sign points to.
     * @return Returns a Vector2 in the direction of the Sign.
     */
    public Vector2 getDirection() {
        return direction;
    }
}