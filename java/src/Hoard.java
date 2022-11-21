import bagel.Image;
import bagel.util.Vector2;

import java.util.Random;

/** This is the Hoard class that handles all Hoard initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Hoard extends Actor implements Depositable {
    private final static Image img = new Image("res/images/hoard.png");
    private int stored = 0;
    private final int TILE_SIZE;

    /** This is the constructor for the Hoard class
     * Creates a Hoard at given x and y co-ordinates and set a final tile size.
     * @param x This is the initial x co-ordinate of the Hoard.
     * @param y This is the initial y co-ordinate of the Hoard.
     */
    public Hoard(int x, int y) {
        super(x, y, img);
        TILE_SIZE = getTileSize();
    }

    /** This method draws the Hoard with it's quantity of fruit in the top left hand corner. */
    @Override
    public void draw() {
        img.drawFromTopLeft(super.getX()*TILE_SIZE,super.getY()*TILE_SIZE);
        super.getFont().drawString(Integer.toString(stored), super.getX()*TILE_SIZE, super.getY()*TILE_SIZE);
    }

    /**
     * This method increases the fruit stored in the Hoard by 1.
     */
    public void deposit() {
        // Increase the held fruit amount by 1
        stored++;
    }

    /**
     * This method decreases the amount of fruit stored in the Hoard if it contains fruit.
     */
    public void steal() {
        if(stored > 0) {
            stored--;
        }
    }

    /**
     * This method returns whether the Hoard has fruit or not.
     * @return Returns true if there is at least one fruit and false otherwise.
     */
    public boolean hasFruit() {
        return(stored > 0);
    }

    /**
     * This method returns the amount of fruit stored by the Hoard.
     * @return Returns an integer of the quantity of fruit.
     */
    public int getFruit() {
        return(stored);
    }

}
