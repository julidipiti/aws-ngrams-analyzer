import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;


public class IOHelperTest {

  /**
   * Tests that the helper prints on a single line over multiple calls.
   */
  @Test
  public void printDefaultOutTest() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);
    PrintStream systemOut = System.out;
    System.setOut(out);

    String str1 = "My string for testing the output.";
    IOHelper.print(str1);
    assertEquals(str1, outputStream.toString());

    String str2 = "More input after a second call to print. Same line.";
    IOHelper.print(str2);
    assertEquals(str1 + str2, outputStream.toString());

    System.out.flush();
    System.setOut(systemOut);
  }

  /**
   * Tests that the helper prints on a custom output, on a single line over multiple calls.
   */
  @Test
  public void printCustomOutTest() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);

    String str1 = "My string for testing the output.";
    IOHelper.print(str1, out);
    assertEquals(str1, outputStream.toString());

    String str2 = "More input after a second call to print. Same line.";
    IOHelper.print(str2, out);
    assertEquals(str1 + str2, outputStream.toString());
  }

  /**
   * Tests that the helper prints on multiple lines over multiple calls.
   */
  @Test
  public void printlnDefaultOutTest() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);
    PrintStream systemOut = System.out;
    System.setOut(out);

    String str1 = "My string for testing the output.";
    IOHelper.println(str1);
    assertEquals(str1 + System.lineSeparator(), outputStream.toString());

    String str2 = "More input after a second call to println. New line.";
    IOHelper.println(str2);
    assertEquals(
        str1 + System.lineSeparator() + str2 + System.lineSeparator(), outputStream.toString());

    System.out.flush();
    System.setOut(systemOut);
  }

  /**
   * Tests that the helper prints on a custom output, on multiple lines over multiple calls.
   */
  @Test
  public void printlnCustomOutTest() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);

    String str1 = "My string for testing the output.";
    IOHelper.println(str1, out);
    assertEquals(str1 + System.lineSeparator(), outputStream.toString());

    String str2 = "More input after a second call to println. New line.";
    IOHelper.println(str2, out);
    assertEquals(
        str1 + System.lineSeparator() + str2 + System.lineSeparator(), outputStream.toString());
  }

  /**
   * Tests that the IOHelper gets an Integer from a custom input properly.
   */
  @Test
  public void getIntegerTest() {
    int num = 2;
    InputStream in =
        new ByteArrayInputStream(
            ("Four" + System.lineSeparator() + "3.14" + System.lineSeparator() + num).getBytes());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);
    int x = IOHelper.getInteger(in, out);

    String expectedErrorOutput = "Number could not be read. Try again." + System.lineSeparator();
    expectedErrorOutput += expectedErrorOutput;

    assertEquals(num, x);
    assertEquals(expectedErrorOutput, outputStream.toString());
  }

  /**
   * Tests that the IOHelper gets a Double from a custom input properly.
   */
  @Test
  public void getDoubleTest() {
    double num = 0.5;
    InputStream in =
        new ByteArrayInputStream(
            ("Four"
                    + System.lineSeparator()
                    + "3e"
                    + System.lineSeparator()
                    + "@#$%"
                    + System.lineSeparator()
                    + num)
                .getBytes());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);
    double x = IOHelper.getDouble(in, out);

    String expectedErrorOutput = "Number could not be read. Try again." + System.lineSeparator();
    expectedErrorOutput += expectedErrorOutput + expectedErrorOutput;

    assertEquals(num, x, 0.01);
    assertEquals(expectedErrorOutput, outputStream.toString());
  }

  /**
   * Tests that the IOHelper gets a single word from a custom input properly.
   */
  @Test
  public void getWordTest() {
    String word = "myWordWithNoSpaces";
    InputStream in =
        new ByteArrayInputStream(
            ("two words"
                    + System.lineSeparator()
                    + "three words string"
                    + System.lineSeparator()
                    + word)
                .getBytes());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);
    String w = IOHelper.getWord(in, out);

    String expectedErrorOutput = "String can not have spaces. Try again." + System.lineSeparator();
    expectedErrorOutput += expectedErrorOutput;

    assertEquals(word, w);
    assertEquals(expectedErrorOutput, outputStream.toString());
  }

  /**
   * Tests that @IOHelper.getWord throws an exception if the input is null.
   */
  @Test(expected = NullPointerException.class)
  public void getWordNullTest() {
    InputStream in = new ByteArrayInputStream("".getBytes());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outputStream);
    IOHelper.getWord(in, out);
  }
}
