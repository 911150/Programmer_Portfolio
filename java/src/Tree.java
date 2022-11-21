import bagel.Image;

/** This is the Tree class that handles all Tree initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Tree extends Actor implements Harvestable {
    private final static Image IMG = new Image("res/images/tree.png");
    private int fruit = 3;
    private final int TILE_SIZE;

    /** This is the constructor for the Tree class
     * Creates a Tree at given x and y co-ordinates and sets the final Tile Size.
     * @param x This is the initial x co-ordinate of the Tree.
     * @param y This is the initial y co-ordinate of the Tree.
     */
    public Tree(int x, int y) {
        super(x, y, IMG);
        TILE_SIZE = getTileSize();
    }

    /** This method draws the Tree with it's quantity of fruit in the top left hand corner. */
    @Override
    public void draw() {
        IMG.drawFromTopLeft(super.getX()*TILE_SIZE,super.getY()*TILE_SIZE);
        getFont().drawString(Integer.toString(fruit), super.getX()*TILE_SIZE, super.getY()*TILE_SIZE);
    }

    /**
     * This method decreases the fruit stored in the Tree by 1 if the Tree has fruit.
     */
    @Override
    public void harvest() {
        if(hasFruit()) {
            fruit--;
        }
    }

    /**
     * This method returns if the Tree has fruit or not.
     * @return This is a boolean regarding the Tree's fruit carrying.
     */
    @Override
    public boolean hasFruit() {
        return(fruit > 0);
    }

}