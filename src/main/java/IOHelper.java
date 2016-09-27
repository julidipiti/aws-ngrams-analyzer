import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

/**
 * Helper to interact with the Input/Output of the program, intending to put together all the
 * interactions with the external world.
 *
 * @author julidipiti
 */
public class IOHelper {

  private static BufferedReader br;

  /**
   * Calls the overloaded print method with an empty string.
   */
  public static void print() {
    print("");
  }

  /**
   * Calls the overloaded print method with the standard output as default.
   *
   * @param str The string to print.
   */
  public static void print(String str) {
    print(str, System.out);
  }

  /**
   * Calls the print method in the specified output to print a string.
   *
   * @param str The string to print.
   * @param out The target output.
   */
  public static void print(String str, PrintStream out) {
    out.print(str);
  }

  /**
   * Calls the overloaded println method with an empty string.
   */
  public static void println() {
    println("");
  }

  /**
   * Calls the overloaded println method with the standard output as default.
   *
   * @param str The string to print.
   */
  public static void println(String str) {
    println(str, System.out);
  }

  /**
   * Calls the println method in the specified output to print a string and a new line.
   *
   * @param str The string to print.
   * @param out The target output.
   */
  public static void println(String str, PrintStream out) {
    out.println(str);
  }

  /**
   * Calls the overloaded method with its default parameters.
   *
   * @return An int from the standard input.
   */
  public static int getInteger() {
    return getInteger(System.in, System.out);
  }

  /**
   * Gets an int from the specified input, and prints in the specified output if there is any error.
   *
   * @param in The input from which to get the int.
   * @param out The output where to print an error, if any.
   * @return An int.
   */
  public static int getInteger(InputStream in, PrintStream out) {
    String line = null;
    br = new BufferedReader(new InputStreamReader(in));
    Integer num = null;
    while (num == null) {
      try {
        line = br.readLine();
        num = Integer.parseInt(line);
      } catch (IOException | NumberFormatException nfe) {
        out.println("Number could not be read. Try again.");
      }
    }
    return num;
  }

  /**
   * Calls the overloaded method with its default parameters.
   *
   * @return A double from the standard input.
   */
  public static double getDouble() {
    return getDouble(System.in, System.out);
  }

  /**
   * Gets an double from the specified input, and prints in the specified output if there is any
   * error.
   *
   * @param in The input from which to get the double.
   * @param out The output where to print an error, if any.
   * @return A double.
   */
  public static double getDouble(InputStream in, PrintStream out) {
    String line = null;
    br = new BufferedReader(new InputStreamReader(in));
    Double num = null;
    while (num == null) {
      try {
        line = br.readLine();
        num = Double.parseDouble(line);
      } catch (IOException | NumberFormatException nfe) {
        out.println("Number could not be read. Try again.");
      }
    }

    return num;
  }

  /**
   * Calls the overloaded method with its default parameters.
   *
   * @return A word from the standard input.
   */
  public static String getWord() {
    return getWord(System.in, System.out);
  }

  /**
   * Gets a single word from the specified input, and prints in the specified output if there is any
   * error.
   *
   * @param in The input from which to get the word.
   * @param out The output where to print an error, if any.
   * @return A single word.
   */
  public static String getWord(InputStream in, PrintStream out) {
    String line = null;
    br = new BufferedReader(new InputStreamReader(in));
    while (line == null) {
      try {
        line = br.readLine();
        if (line.split(" ").length != 1) {
          line = null;
          out.println("String can not have spaces. Try again.");
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
        return null;
      }
    }

    return line;
  }
}
