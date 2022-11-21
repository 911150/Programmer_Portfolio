import bagel.Image;
import bagel.util.Vector2;

import java.util.ArrayList;

/** This is the Gatherer class that handles all Gatherer initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Gatherer extends Actor implements Moveable {
    private final static Image IMG = new Image("res/images/gatherer.png");
    private final static int NEGATIVE = -1;
    // Algorithm 1: Gatherer initialisation;
    private final static int X_INIT = -1;
    private final static int Y_INIT = 0;
    private Vector2 direction = new Vector2(X_INIT, Y_INIT);
    private double angle = Math.PI;
    private boolean carrying = false;


    /** This is the constructor for the Gatherer class
     * Creates a gatherer, initialises it, and sets the constant size of the tiles.
     * @param x This is the initial x co-ordinate of the Gatherer
     * @param y This is the initial y co-ordinate of the Gatherer
     */
    public Gatherer(int x, int y) {
        super(x, y, IMG);
        super.setActive(true);
    }

    /** This is the move method for the Gatherer. It moves the Gatherer one tile based on direction. */
    @Override
    public void move() {
        if(isActive()) {
            super.setX(super.getX() + (int)direction.asPoint().x);
            super.setY(super.getY() + (int)direction.asPoint().y);
        }
    }

    /** This method rotates the direction of the Gatherer using trigonometry.
     * @param o This is the rotation to be applied to the Gatherer (Clockwise).
     */
    public void rotate(double o) {
        angle -= o;
        this.direction = new Vector2(Math.cos(angle), -Math.sin(angle));
    }

    /** This method rotates the direction of the Gatherer to face a given direction *As opposed to rotating by an amount in radians
     *
     * @param v This is a Vector2 that will be the Gatherer's new direction.
     */
    public void rotate(Vector2 v) {
        direction = v;
        angle = Math.atan2(-direction.y,direction.x);
    }

    /** This method moves the Gatherer to its position last tick and sets it's active state to false. */
    // if on fence -> direction = null -> set active false
    public void interactFence() {
        direction = direction.mul(NEGATIVE);
        move();
        setActive(false);
    }


    /** This method completes the mitosis pool functionality as defined in the specification.
     *  It uses an ArrayList to buffer the new Gatherers being created.
     * @param load Used to store new Gatherers to be added.
     */
    public void interactPool(ArrayList<Actor> load) {
        // Create two new Actors of type child type Gatherer.
        Actor a = new Gatherer(this.getX(), this.getY());
        Actor b = new Gatherer(this.getX(), this.getY());
        // Set new Gatherer's direction = this direction, rotate each respectively, move each, load each into buffer.
        a.setDirection(direction);
        b.setDirection(direction);
        a.rotate(-Math.PI/2);
        b.rotate(Math.PI/2);
        a.move();
        b.move();
        load.add(a);
        load.add(b);
    }

    /** This method rotates the Gatherer in the direction of the sign it is standing on.
     * @param i This is the Sign the Gatherer is standing on.
     */
    // if on sign -> direction = sign direction
    public void interactSign(Actor i) {
        rotate(i.getDirection());
    }

    /** This method is called when the Gatherer is standing on a Tree (Golden or Regular).
     *  If the Gatherer isn't carrying, it attempts to harvest from the tree.
     * @param i This is the Tree that the Gatherer is standing on.
     */
    public void interactTree(Actor i) {
        /* if map_tile contains tree & tree has fruit -> gath carry = true, tree -= fruit
           if is tree -> tree has fruit? -> carry = true, harvest tree, reverse direction */
        if(!carrying) {
            if (i instanceof Tree) {
                if(((Tree) i).hasFruit()) {
                    carrying = true;
                    ((Tree) i).harvest();
                    rotate(Math.PI);
                }
            } else {
                carrying = true;
                rotate(Math.PI);
            }
        }
    }

    /** This is the method called when the Gatherer is on a Pile (Stockpile or Hoard).
     *  Gatherer will attempt to deposit carrying fruit.
     * @param i  This is the pile that the Gatherer is standing on.
     */
    // if exists pile -> if !gath-carrying -> pile increase -> rotate gath
    public void interactPile(Actor i) {
        rotate(Math.PI);
        if(carrying) {
            carrying = false;
            if(i instanceof Stockpile) {
                ((Stockpile) i).deposit();
            } else if (i instanceof Hoard) {
                ((Hoard) i).deposit();
            }
        }
    }

    /** This returns the value of the Gatherer's active status.
     *
     * @return Returns true/false depending on if the Gatherer is active or not.
     */
    public boolean isActive() {
        return super.getActive();
    }

    /** This method returns the current direction of the Gatherer.
     *
     * @return Returns a Vector2 vector in the direction of the Gatherer.
     */
    public Vector2 getDirection() {
        return direction;
    }

    /** This method sets the direction of the Gatherer.
     *
     * @param direction This is a Vector2 that is set as the Gatherers' direction.
     */
    public void setDirection(Vector2 direction) {
        this.direction = direction;
        this.angle = Math.atan2(-direction.asPoint().y, direction.asPoint().x);
    }

}
