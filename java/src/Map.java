import java.util.ArrayList;

/** This is the Map class.
 * @author Noah
 * @version 2
 */

public class Map implements Drawable {
    private static Tile[][] map;
    private static final int TILE_SIZE = 64;

    /** This is the constructor for the Map class
     * Creates a map of size needed, initialises it.
     * @param x This is the initial size of the map.
     * @param y This is the initial size of the map.
     */
    public Map(double x, double y) {
        int xmax = (int)x/TILE_SIZE;
        int ymax = (int)y/TILE_SIZE;

        map = new Tile[(int)(x/TILE_SIZE)][(int)y/TILE_SIZE];
        // initialise map
        for(int i=0;i<xmax;i++) {
            for(int j=0;j<ymax;j++) {
                Tile t = new Tile(i * TILE_SIZE, j * TILE_SIZE);
                map[i][j] = t;
            }
        }
    }

    /**
     * This method handles the updating of the map.
     */
    public void run() {
        // Move actors -> Update the map -> Check for interactions.
        moveActors();
        updateMap();
        updateState();
    }


    /** This method handles adding new actors to the map.
     * @param a The actor you want to add to the map.
     */
    public void addToMap(Actor a) {
        int x = a.getX();
        int y = a.getY();
        map[x][y].add(a);
    }

    /**
     * This method handles the drawing of the map.
     */
    @Override
    public void draw() {
        for(Tile[] x : map) {
            for(Tile xy : x) {
                    xy.draw();
            }
        }
    }

    /**
     * This method handles the moving of actors.
     */
    public void moveActors() {
        for(Tile[] x : map) {
            for (Tile map_tile : x) {
                map_tile.move();
            }
        }
    }


    /**
     * This method handles updating the state of the map. Performed post moving the actors.
     */
    public void updateState() {
         for(Tile[] x : map) {
                for(Tile map_tile : x) {
                    map_tile.tick();
                }
            }
        }

    /**
     * This method handles stopping the game once the actors have stopped moving.
      * @return Boolean true if ready to stop and false otherwise.
     */
    public boolean halt() {
        for(Tile[] x : map) {
            for(Tile map_tile : x) {
                for(Actor a : map_tile.getActors()) {
                    if(a.getActive()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * This method checks to see if all of the actors are on the correct tiles and moves them otherwise.
     */
    // find the actors that are out of place.
    public void updateMap() {
        for(Tile[] x : map) {
            for(Tile xy : x) {
                // Moving of the actors to their new tiles
                ArrayList<Actor> temp = new ArrayList<>();
                for(Actor a : xy.getActors()) {
                    if(a.getX() != xy.getX()/TILE_SIZE || a.getY() != xy.getY()/TILE_SIZE) {
                        // Find actors that are out of position and add them to arraylist
                        temp.add(a);
                    }
                }
                for(Actor a : temp) {
                    // move actor to new map tile position
                    addToMap(a);
                    xy.remove(a);
                }
            }
        }
    }
}
