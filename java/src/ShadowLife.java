import bagel.*;
import bagel.util.Colour;
import bagel.util.Point;
import bagel.util.Vector2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** This is the ShadowLife class that initialises the game and handles the updating of the game state.
 * @author Noah
 * @version 2
 */

public class ShadowLife extends AbstractGame {
    private static final String bg_loc = "res/images/background.png";
    private static int width, height, tick_rate, max_ticks;
    private static String world;
    private final Image background = new Image(bg_loc);
    private final ArrayList<Actor> actors;
    private final Map m;
    private long last_tick = new Date().getTime();
    private int dummy_tick = 0;

    private static final int MAX_ARGS = 3;
    private static final int MIN_TICK_RATE = 0;
    private static final int SYSTEM_EXIT_FAILURE = -1;
    private static final int SYSTEM_EXIT_SUCCESS = 0;
    private static final int TILE_SIZE = 64;

    private static final int BACKGROUND_ORIGIN_X = 0;
    private static final int BACKGROUND_ORIGIN_Y = 0;


    /** This is the constructor for the ShadowLife class
     *  Initialises the game state by creating a world map and reading in the actors from file into the map.
     */
    public ShadowLife() {
        super(width, height, "Project 2");
        // Initialise game state
        actors = readActors(world);
        m  = new Map(width, height);

        for(Actor a : actors) {
            m.addToMap(a);
        }

    }

    /** This is the main method of the ShadowLife class that checks for incorrect arguments and starts the game
     *  if there is valid input.
     * @param args Handling of the arguments
     * @throws IOException Handles file operation exceptions.
     */
    public static void main(String[] args) throws IOException {
        // Handling of background image dimensions.
        BufferedImage bimg = ImageIO.read(new File(bg_loc));
        width = bimg.getWidth();
        height = bimg.getHeight();

        // Checking for valid input.
        if (args.length == MAX_ARGS) {
            try {
                tick_rate = Integer.parseInt(args[0]);
                max_ticks = Integer.parseInt(args[1]);
                world = args[2];
                if(tick_rate < MIN_TICK_RATE || max_ticks < MIN_TICK_RATE) {
                    System.out.println("usage: ShadowLife <tick rate> <max ticks> <world file>");
                    System.exit(SYSTEM_EXIT_FAILURE);
                }

            } catch (NumberFormatException e) {
                System.out.println("usage: ShadowLife <tick rate> <max ticks> <world file>");
                System.exit(SYSTEM_EXIT_FAILURE);
            }
        } else {
            System.out.println("usage: ShadowLife <tick rate> <max ticks> <world file>");
            System.exit(SYSTEM_EXIT_FAILURE);
        }

        // Start the game.
        ShadowLife game = new ShadowLife();
        game.run();
    }


    /** This method is the back-bone of the game. It handles the game tick mechanics and is responsible for
     *  updating the individual components of the game and halting the game when certain criterion are met.
     * @param input This is input passed in by the user.
     */
    @Override
    public void update(Input input) {
        long time = getTime();
        // Drawing of board and actors
        background.drawFromTopLeft(BACKGROUND_ORIGIN_X, BACKGROUND_ORIGIN_Y);
        m.draw();

        // Tick mechanics, with timeout
        if ((time  - last_tick) >= tick_rate) {
            last_tick = time;
            if(dummy_tick > max_ticks) {
                System.out.println("Timed out");
                System.exit(SYSTEM_EXIT_FAILURE);
            }
            // If all moveable actors are no longer active.
            if(!m.halt()) {
                m.run();
                dummy_tick++;
            } else {
                printResults();
            }
        }
    }

    /** This method prints the results of the simulation to console. */
    public void printResults() {
        System.out.println(dummy_tick + " ticks");
        // For all actors that store fruit print their amounts in the order they were added.
        for(Actor a : actors) {
            if(a instanceof Stockpile) {
                int fruit = ((Stockpile) a).getFruit();
                System.out.println(fruit);
            } else if (a instanceof Hoard) {
                int fruit = ((Hoard) a).getFruit();
                System.out.println(fruit);
            }
        }
        System.exit(SYSTEM_EXIT_SUCCESS);
    }


    /** This method reads in a list of actors from a csv file and initialising them into an initial array.
     * @param csv This is the location of the world file (.csv)
     * @return Returns an ArrayList of actors read in from the world file.
     */
    //Takes a file of specified format and returns an Array of Actors built from the contents
    public static ArrayList<Actor> readActors(String csv) {
        ArrayList<Actor> temp = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
            String line;
            int line_counter = 0;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                // Checks for valid input.
                if(values.length != MAX_ARGS || !isStringInteger(values[1]) || !isStringInteger(values[2])) {
                    System.out.println("error: file \"" + csv + "\" at line " + line_counter);
                    System.exit(SYSTEM_EXIT_FAILURE);
                }

                // Loading actors of respective type from world file
                switch (values[0]) {
                    case "Tree": {
                        Actor a = new Tree(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "GoldenTree": {
                        Actor a = new GoldenTree(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "Stockpile": {
                        Actor a = new Stockpile(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "Hoard": {
                        Actor a = new Hoard(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "Pad": {
                        Actor a = new Pad(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "Fence": {
                        Actor a = new Fence(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "SignUp": {
                        Actor a = new Sign(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE,
                                new Image("res/images/up.png"), new Vector2(0, -1));
                        temp.add(a);
                        break;
                    }
                    case "SignDown": {
                        Actor a = new Sign(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE,
                                new Image("res/images/down.png"), new Vector2(0, 1));
                        temp.add(a);
                        break;
                    }
                    case "SignLeft": {
                        Actor a = new Sign(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE,
                                new Image("res/images/left.png"), new Vector2(-1, 0));
                        temp.add(a);
                        break;
                    }
                    case "SignRight": {
                        Actor a = new Sign(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE,
                                new Image("res/images/right.png"), new Vector2(1, 0));
                        temp.add(a);
                        break;
                    }
                    case "Pool": {
                        Actor a = new Pool(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "Gatherer": {
                        Actor a = new Gatherer(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    case "Thief": {
                        Actor a = new Thief(Integer.parseInt(values[1]) / TILE_SIZE, Integer.parseInt(values[2]) / TILE_SIZE);
                        temp.add(a);
                        break;
                    }
                    default: {
                        System.out.println("error: file \"" + csv + "\" at line " + line_counter);
                        System.exit(SYSTEM_EXIT_FAILURE);
                    }
                }
                line_counter++;
            }

        // Handle the case of file error.
        } catch (IOException e) {
            System.out.println("error: \"" + csv + "\" not found");
            System.exit(SYSTEM_EXIT_FAILURE);
        }
        return temp;
    }

    /** Checks if a String can be converted to an integer.
     * @param number The string to check.
     * @return True if yes, false if no.
     */
    public static boolean isStringInteger(String number ){
        try{
            Integer.parseInt(number);
        }catch(Exception e ){
            return false;
        }
        return true;
    }

    // Outside reference to getTime was needed.
    /** This method gets the current time in milliseconds.
     * @return Returns the current time in milliseconds
     */
    public long getTime() {
        return (new Date().getTime());
    }

}
