import bagel.Font;
import bagel.Image;
import bagel.util.Vector2;

/* Actor class using polymorphism. */
/** This is an Abstract Actor class that handles all it's Child classes implementation
 *  Stores relative information about the child, including it's image, active status, and location in the Map.
 * @author Noah
 * @version 2
 */

public abstract class Actor implements Drawable {
    private final Font font = new Font("res/VeraMono.ttf", FONT_SIZE);
    private static final int FONT_SIZE = 20;
    private static final int TILE_SIZE = 64;
    private final Image image;
    private boolean active = false;
    private int x,y;


    /** This is the constructor for the Actor class
     * Creates an Actor, initialises it.
     * @param x This is the initial x co-ordinate of the Actor.
     * @param y This is the initial y co-ordinate of the Actor.
     * @param image This is the image file the Actor class will project.
     */
    public Actor(int x, int y, Image image) {
        this.x = x;
        this.y = y;
        this.image = image;
    }

    /** This method draws the Actor at it's location using Bagel draw method.*/
    public void draw() {
        image.drawFromTopLeft(x*TILE_SIZE,y*TILE_SIZE);
   }

    /** This method gets the x co-ordinate of the Actor.
     * @return Returns an integer of the x co-ordinate.
     */
    public int getX() {
        return x;
    }

    /** This method sets the x co-ordinate of the Actor.
     * @param x This is the new x co-ordinate as an integer.
     */
    public void setX(int x) {
        this.x = x;
    }

    /** This method gets the y co-ordinate of the Actor.
     * @return Returns an integer of the y co-ordinate.
     */
    public int getY() {
        return y;
    }

    /** This method sets the y co-ordinate of the Actor.
     * @param y This is the new y co-ordinate as an integer.
     */
    public void setY(int y) {
        this.y = y;
    }

    /** This method returns the font stored in the Actor class.
     * @return Returns a Font defined in the Actor class.
     */
    public Font getFont() {
        return(font);
    }

    /** This method returns the constant tile size defined in the Actor class.
     * @return  Returns an integer of the tile size.
     */
    public int getTileSize() {
        return TILE_SIZE;
    }

    /** This method returns the Active status of the Actor.
     * @return Returns a boolean of the Active status of the respective Actor.
     */
    public boolean getActive() {
        return active;
    }

    /** This method sets the Active status of the Actor.
     * @param b This is the status that the Actor will have it's Active set too.
     */
    public void setActive(boolean b) {
        active = b;
    }

    /** This method is making use of overloading, so the move method can be called on all Actor children.
     *  As a default the move method does nothing.
     */
    public void move() {
        //Default method; do nothing
    }

    /** This method returns the current direction of the Actor.
     * @return Returns a Vector2 vector in the direction of the Actor.
     */
    public Vector2 getDirection() {
        //Do nothing, this code should never be run
        return null;
    }

    /** This method sets the direction of the Actor.
     *
     * @param direction This is a Vector2 that is set as the Actors' direction.
     */
    public void setDirection(Vector2 direction) {
        //Do nothing, this code should never be run
    }

    /** This method is making use of overloading, so the rotate method can be called on all Actor children.
     *  As a default the rotate method does nothing.
     * @param d double that would rotate the Actor if implemented.
     */
    public void rotate(double d) {
        //Do nothing, this code should never be run
    }

}
