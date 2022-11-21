import java.util.ArrayList;

/** This is the Tile class, used to fill the map.
 * @author Noah
 * @version 2
 */

public class Tile implements Drawable {
    private final int x,y;
    private final ArrayList<Actor> tile = new ArrayList<>();

    /** This is the constructor for the Tile class
     * Creates an Tile, initialises it.
     * @param x This is the initial x co-ordinate of the position in the array.
     * @param y This is the initial y co-ordinate of the position in the array.
     */
    public Tile(int x, int y) {
        this.x = x;
        this.y = y;
    }

    /** This method add's an Actor to this Tile.
     * @param a The actor to be added.
     */
    public void add(Actor a) {
        tile.add(a);
    }

    /** This method remove's an Actor from this Tile.
     * @param a The actor to be removed.
     */
    public void remove(Actor a) {
        tile.remove(a);
    }

    // Making check type universal

    /** This method iterates through the list of actors in the Tile, and returns the first it finds of particular type.
     * @param actors The Array of actors to search through
     * @param type The type of class to search for
     * @return The first actor of type class found in the array or null if no such actor exists.
     */
    // Using generics to check if the an ArrayList contains an Actor of a particular type and returns first.
    public Actor isInstance(ArrayList<Actor> actors, Class<?> type) {
        for (Actor a : actors) {
            if (type.isInstance(a)) {
                return a;
            }
        }
        return null;
    }

    /**
     * This handles the moving of all actors on this tile.
     */
    public void move() {
        for(Actor a : tile) {
            a.move();
        }
    }

    /** This method is called to update all the respective actors on the Tile.
     *  It handles interactions between the different types of Actors.
     */
    public void tick() {
        // Initialise buffer.
        ArrayList<Actor> load = new ArrayList<>();
        ArrayList<Actor> delete = new ArrayList<>();

        for(Actor a : tile) {
            Actor temp;

            // Algorithm 2 - Gatherer
            // mitosis pool --> sign --> tree --> hoard/stockpile --> fence --> move
            if(a instanceof Gatherer) {
                Gatherer gatherer = (Gatherer) a;

                // If Gatherer is standing on Fence.
                if(isInstance(tile, Fence.class) != null) {
                    gatherer.interactFence();
                }

                // If Gatherer is standing on Pool.
                if(isInstance(tile, Pool.class) != null) {
                    gatherer.interactPool(load);
                    delete.add(a);
                }

                // If Gatherer is standing on Sign.
                if((temp = isInstance(tile, Sign.class)) != null) {
                    gatherer.interactSign(temp);
                }

                // If Gatherer is standing on Tree.
                if(((temp = isInstance(tile, Tree.class)) != null) || ((temp = isInstance(tile, GoldenTree.class)) != null)) {
                    gatherer.interactTree(temp);
                }

                // If Gatherer is standing on Pile.
                if(((temp = isInstance(tile, Stockpile.class)) != null) || (temp = isInstance(tile, Hoard.class)) != null) {
                    gatherer.interactPile(temp);
                }
            }

            // Algorithm 4 - Thief
            if(a instanceof Thief) {
                // Typecasting of thief.
                Thief thief = (Thief) a;

                if(isInstance(tile, Pool.class) != null) {
                    // Cache relevant actors to load / delete
                    thief.interactPool(load);
                    delete.add(a);
                }

                // If thief is on a Fence.
                if(((isInstance(tile, Fence.class)) != null)) {
                    thief.interactFence();
                }

                // If thief is on a sign.
                if((temp = isInstance(tile, Sign.class)) != null) {
                    thief.interactSign(temp);
                }


                // If thief is on a pad.
                if((isInstance(tile, Pad.class)) != null) {
                    thief.interactPad();
                }

                // If thief is on a gatherer.
                if((temp = isInstance(tile, Gatherer.class)) != null) {
                    // Added a check, since gatherer moves first, have to check new pos
                    thief.interactGatherer(temp);
                }

                // If thief is on a tree.
                if(((temp = isInstance(tile, Tree.class)) != null) ||
                        ((temp = isInstance(tile, GoldenTree.class)) != null)) {
                    thief.interactTree(temp);
                }


                // If thief is on a Hoard.
                if((temp = isInstance(tile, Hoard.class)) != null) {
                    thief.interactHoard(temp);
                }

                // If thief is on a stockpile.
                if((temp = isInstance(tile, Stockpile.class)) != null) {
                    thief.interactPile(temp);
                }
            }
        }
        // Apply buffered operations.
        tile.addAll(load);
        tile.removeAll(delete);
    }


    /**
     * Draw the actors on the given tile by calling their draw method.
     */
    // Draw actors placed on the tile
    @Override
    public void draw() {
        for(Actor a : tile) {
            a.draw();
        }
    }

    /** Gets the actors on the tile.
     *
     * @return Returns an arraylist of actors
     */
    public ArrayList<Actor> getActors() {
        return tile;
    }

    /**
     * Gets the X co-ordinate of the Tile
     * @return Returns an integer of the X co-ordinate.
     */
    public int getX() {
        return x;
    }

    /**
     * Gets the Y co-ordinate of the Tile
     * @return Returns an integer of the X co-ordinate.
     */
    public int getY() {
        return y;
    }
}
