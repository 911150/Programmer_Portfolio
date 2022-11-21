import bagel.Image;
import bagel.util.Vector2;

import java.util.Random;

/** This is the Stockpile class that handles all Stockpile initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Stockpile extends Actor implements Depositable {
    private final static Image IMG = new Image("res/images/cherries.png");
    private int stored = 0;
    private final int TILE_SIZE;

    /** This is the constructor for the Stockpile class
     * Creates a Stockpile at given x and y co-ordinates and set a final tile size.
     * @param x This is the initial x co-ordinate of the Stockpile.
     * @param y This is the initial y co-ordinate of the Stockpile.
     */
    public Stockpile(int x, int y) {
        super(x, y, IMG);
        TILE_SIZE = getTileSize();
    }

    /** This method draws the Stockpile with it's quantity of fruit in the top left hand corner. */
    @Override
    public void draw() {
        IMG.drawFromTopLeft(super.getX()*TILE_SIZE,super.getY()*TILE_SIZE);
        getFont().drawString(Integer.toString(stored), super.getX()*TILE_SIZE, super.getY()*TILE_SIZE);
    }

    /**
     * This method increases the fruit stored in the Stockpile by 1.
     */
    public void deposit() {
        // Increase the held fruit amount by 1
        stored++;
    }

    /**
     * This method returns whether the Stockpile has fruit or not.
     * @return Returns true if there is at least one fruit and false otherwise.
     */
    public boolean hasFruit() {
        return(stored > 0);
    }

    /**
     * This method decreases the amount of fruit stored in the Stockpile if it contains fruit.
     */
    public void steal() {
        stored--;
    }

    /**
     * This method returns the amount of fruit stored by the Stockpile.
     * @return Returns an integer of the quantity of fruit.
     */
    public int getFruit() {
        return(stored);
    }

}
