import bagel.Image;
import bagel.util.Vector2;

import java.util.ArrayList;

/** This is the Thief class that handles all Thief initialisation and implementation
 * @author Noah
 * @version 2
 */

public class Thief extends Actor implements Moveable {
    private final static Image IMG = new Image("res/images/thief.png");
    private static final double  NINETY_DEGREES = Math.PI/2;

    // Algorithm 3: Thief initialisation
    private double angle = NINETY_DEGREES;
    private static final int X_INIT = 0;
    private static final int Y_INIT = -1;
    private static final int NEGATIVE = -1;


    private Vector2 direction = new Vector2(X_INIT, Y_INIT);
    private boolean carrying = false;
    private boolean consuming = false;

    /** This is the constructor for the Thief class
     * Creates a Thief, initialises it.
     * @param x This is the initial x co-ordinate of the Thief
     * @param y This is the initial y co-ordinate of the Thief
     */
    public Thief(int x, int y) {
        super(x, y, IMG);
        super.setActive(true);
    }


    /** This is the move method for the Thief. It moves the Thief one tile based on direction. */
    @Override
    public void move() {
        if(isActive()) {
            super.setX(super.getX() + (int)direction.asPoint().x);
            super.setY(super.getY() + (int)direction.asPoint().y);
        }
    }

    /** This method rotates the direction of the Thief using trigonometry.
     *
     * @param o This is the rotation to be applied to the Thief (Clockwise).
     */
    public void rotate(double o) {
        angle -= o;
        direction = new Vector2(Math.cos(angle), -Math.sin(angle));
    }

    /** This method rotates the direction of the Thief to face a given direction *As opposed to rotating by an amount in radians
     *
     * @param v This is a Vector2 that will be the Thief new direction.
     */
    public void rotate(Vector2 v) {
        direction = v;
        angle = Math.atan2(-direction.y,direction.x);
    }

    /** This method moves the Thief to its position last tick and sets it's active state to false. */
    public void interactFence() {
        direction = direction.mul(NEGATIVE);
        move();
        setActive(false);
    }

    /** This method completes the mitosis pool functionality as defined in the specification.
     *  It uses an ArrayList to buffer the new Thief's being created.
     * @param load Used to store new Thief's to be added.
     */
    public void interactPool(ArrayList<Actor> load) {
        Actor a = new Thief(this.getX(), this.getY());
        Actor b = new Thief(this.getX(), this.getY());
        // Set new Thief's direction = this direction, rotate each respectively, move each, load each into buffer.
        a.setDirection(direction);
        b.setDirection(direction);
        a.rotate(-NINETY_DEGREES);
        b.rotate(NINETY_DEGREES);
        a.move();
        b.move();
        load.add(a);
        load.add(b);
    }

    /** This method rotates the Thief in the direction of the sign it is standing on.
     * @param i This is the Sign the Thief is standing on.
     */
    // if thief on sign -> set direction to sign
    public void interactSign(Actor i) {
        rotate(i.getDirection());
    }

    /** This method sets the Thief's consuming status to true.
     *  This method is called when Thief is standing on a Pad */
    // if thief on pad -> set consuming true
    public void interactPad() {
        consuming = true;
    }

    /** This method rotate's the Thief by 270 degrees.
     *  This method is called when the Thief is standing on a Gatherer.
     * @param i This is the Gatherer the Thief is standing on.
     */
    // if thief on gatherer -> clockwise 3pi/2
    public void interactGatherer(Actor i) {
        // Added a check, since gatherer moves first, have to check new pos
        if(i.getX() == this.getX() && i.getY() == this.getY()) {
            rotate(3*NINETY_DEGREES);
        }
    }

    /** This method is called when the Thief is standing on a Tree (Golden or Regular).
     *  If the Thief isn't carrying, it attempts to steal from the tree.
     * @param i This is the Tree that the Thief is standing on.
     */
    // if thief on tree -> try harvest
    public void interactTree(Actor i) {
        if(!carrying) {
            if (i instanceof Tree) {
                if(((Tree) i).hasFruit()) {
                    carrying = true;
                    ((Tree) i).harvest();
                }
            } else {
                carrying = true;
            }
        }
    }

    /** This method is called when the Thief is standing on a Hoard.
     *  This method attempts to steal from the hoard if it is not carrying, and rotates 90 degrees if the Hoard has no fruit.
     *  If the Thief is carrying, it will attempt to deposit into the Hoard and rotate 90 degrees.
     * @param i This is the Hoard that the Thief is standing on.
     */
    // if thief on hoard -> if consuming = true -> set cons false ->
    // if consuming = false -> if hoard hasfruit -> set carry true -> decrease horde
    public void interactHoard(Actor i) {
        if(consuming) {
            consuming = false;
            if(!carrying) {
                if(((Hoard) i).hasFruit()) {
                    carrying = true;
                    ((Hoard) i).steal();
                } else {
                    rotate(NINETY_DEGREES);
                }
            }
        } else if (carrying) {
            carrying = false;
            ((Hoard) i).deposit();
            rotate(NINETY_DEGREES);
        }
    }

    /** This is the method called when the Thief is on a Pile (Stockpile or Hoard).
     *  Thief will attempt to deposit carrying fruit.
     * @param i  This is the pile that the Thief is standing on.
     */
    //if thief on stockpile -> if carry false -> if stock has fruit ->
    // set carry true, set cons false, decrease stock, rotate PI/2
    public void interactPile(Actor i) {
        if(!carrying) {
            if(((Stockpile) i).hasFruit()) {
                carrying = true;
                consuming = false;
                ((Stockpile) i).steal();
                rotate(NINETY_DEGREES);
            }
        } else {
            rotate(NINETY_DEGREES);
        }
    }

    /** This returns the value of the Thief active status.
     *
     * @return Returns true/false depending on if the Thief is active or not.
     */
    public boolean isActive() {
        return super.getActive();
    }

    /** This sets the status of the Thief to a boolean.
     * @param active This is the boolean the Thief will have it's active status set too.
     */
    public void setActive(boolean active) {
        super.setActive(active);
    }

    /** This method returns the current direction of the Thief.
     *
     * @return Returns a Vector2 vector in the direction of the Thief.
     */
    public Vector2 getDirection() {
        return direction;
    }

    /** This method sets the direction of the Thief.
     *
     * @param direction This is a Vector2 that is set as the Thief direction.
     */
    public void setDirection(Vector2 direction) {
        this.direction = direction;
        this.angle = Math.atan2(-direction.asPoint().y, direction.asPoint().x);
    }

}
