package org.apache.pinot.fsa;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.pinot.fsa.builders.FSA5Serializer;
import org.apache.pinot.fsa.builders.FSABuilder;
import org.apache.pinot.fsa.utils.RegexpMatcher;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class FSALargeStressTest extends TestBase {
  private static byte[][] inputData;
  private FSA fsa;
  boolean initialized;

  @BeforeClass
  public static void setUp() throws Exception {
    Set<String> inputStrings = new HashSet<>();
    InputStream fileInputStream;
    InputStreamReader inputStreamReader;
    BufferedReader bufferedReader;

    File directory = new File("./src/test/resources/cocacorpus/");

    for (final File fileEntry : directory.listFiles()) {
      fileInputStream = new FileInputStream(fileEntry);
      inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
      bufferedReader = new BufferedReader(inputStreamReader);

      String currentLine;
      while ((currentLine = bufferedReader.readLine()) != null) {
        String[] tmp = currentLine.split("\\s+");    //Split space
        for (String currentWord : tmp) {
          inputStrings.add(currentWord);
        }
      }
    }

    inputData = convertToBytes(inputStrings);

    Arrays.sort(inputData, FSABuilder.LEXICAL_ORDERING);
  }

  @Before
  public void initialize()
      throws IOException {
    final int min = 200;
    final int max = 400;

    FSABuilder fsaBuilder = new FSABuilder();

    for (byte[] currentArray : inputData) {
      fsaBuilder.add(currentArray, 0, currentArray.length, (int) (Math.random() * (max - min + 1) + min));
    }

    fsa = fsaBuilder.complete();

    final byte[] fsaData = new FSA5Serializer().withNumbers().serialize(fsa, new ByteArrayOutputStream()).toByteArray();

    File outputFile = new File("/Users/atrisharma/bigbigdude.txt");

    if (initialized != true) {
      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        fos.write(fsaData);
        initialized = true;
      }
    }
  }

  @Test
  public void testRegex1() throws IOException {
    assertEquals(207, regexQueryNrHits("q.[aeiou]c.*"));
  }

  @Test
  public void testRegex3() throws IOException {
    assertEquals(20858, regexQueryNrHits("b.*"));
  }

  @Test
  public void testRegex4() throws IOException {
    assertEquals(1204544, regexQueryNrHits("~#"));
  }

  @Test
  public void testRandomWords() throws IOException {
    assertEquals(1, regexQueryNrHits("respuestas"));
    assertEquals(1, regexQueryNrHits("Berge"));
    assertEquals(1, regexQueryNrHits("\\@qwx198595"));
    assertEquals(1, regexQueryNrHits("popular"));
    assertEquals(1, regexQueryNrHits("Montella"));
    assertEquals(1, regexQueryNrHits("notably"));
    assertEquals(1, regexQueryNrHits("accepted"));
    assertEquals(1, regexQueryNrHits("challenging"));
    assertEquals(1, regexQueryNrHits("insurance"));
    assertEquals(1, regexQueryNrHits("Calls"));
    assertEquals(1, regexQueryNrHits("certified"));
    assertEquals(1, regexQueryNrHits(".*196169"));
    assertEquals(4299, regexQueryNrHits(".*wx.*"));
    assertEquals(1, regexQueryNrHits("keeps"));
    assertEquals(1, regexQueryNrHits("\\@qwx160430"));
    assertEquals(1, regexQueryNrHits("called"));
    assertEquals(1, regexQueryNrHits("Rid"));
    assertEquals(1, regexQueryNrHits("Computer"));
    assertEquals(1, regexQueryNrHits("\\@qwx871194"));
    assertEquals(1, regexQueryNrHits("control"));
    assertEquals(1, regexQueryNrHits("Gassy"));
    assertEquals(1, regexQueryNrHits("Nut"));
    assertEquals(1, regexQueryNrHits("Strangle"));
    assertEquals(1, regexQueryNrHits("ANYTHING"));
    assertEquals(1, regexQueryNrHits("RiverMusic"));
    assertEquals(1, regexQueryNrHits("\\@qwx420154"));
  }

  /**
   * Return all matches for given regex
   */
  private long regexQueryNrHits(String regex) throws IOException {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, fsa);

    return resultList.size();
  }

  private static byte[][] convertToBytes(Set<String> strings) {
    byte[][] data = new byte[strings.size()][];

    Iterator<String> iterator = strings.iterator();

    int i = 0;
    while (iterator.hasNext()) {
      String string = iterator.next();
      data[i] = string.getBytes(Charset.defaultCharset());
      i++;
    }
    return data;
  }
}
