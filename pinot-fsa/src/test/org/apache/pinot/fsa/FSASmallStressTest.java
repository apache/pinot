package org.apache.pinot.fsa;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.fsa.builders.FSABuilder;
import org.junit.Before;
import org.junit.Test;

import static org.apache.pinot.fsa.FSATestUtils.convertToBytes;
import static org.apache.pinot.fsa.FSATestUtils.regexQueryNrHits;
import static org.junit.Assert.assertEquals;

public class FSASmallStressTest extends TestBase {
  private FSA fsa;

  @Before
  public void setUp() throws Exception {
    Set<String> inputStrings = new HashSet<>();
    InputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;

    File file = new File("./src/test/resources/words.txt");

    fileInputStream = new FileInputStream(file);
    inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
    bufferedReader = new BufferedReader(inputStreamReader);

    String currentWord;
    while((currentWord = bufferedReader.readLine()) != null) {
      inputStrings.add(currentWord);
    }

    byte[][] bytesArray = convertToBytes(inputStrings);

    Arrays.sort(bytesArray, FSABuilder.LEXICAL_ORDERING);

    FSABuilder fsaBuilder = new FSABuilder();

    for (byte[] currentArray : bytesArray) {
      fsaBuilder.add(currentArray, 0, currentArray.length, -1);
    }

    fsa = fsaBuilder.complete();

    /*final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(fsa, new ByteArrayOutputStream())
            .toByteArray();

    try (FileOutputStream fos = new FileOutputStream("/Users/atrisharma/foobaaaar.txt")) {
      fos.write(fsaData);*/
  }

  @Test
  public void testRegex1() throws IOException {
    assertEquals(127, regexQueryNrHits("q.[aeiou]c.*", fsa));
  }

  @Test
  public void testRegex2() throws IOException {
    assertEquals(24370, regexQueryNrHits("a.*", fsa));
  }

  @Test
  public void testRegex3() throws IOException {
    assertEquals(18969, regexQueryNrHits("b.*", fsa));
  }

  @Test
  public void testRegex4() throws IOException {
    assertEquals(466550, regexQueryNrHits("~#", fsa));
  }

  @Test
  public void testRegex5() throws IOException {
    assertEquals(1, regexQueryNrHits(".*landau", fsa));
  }

  @Test
  public void testRegex6() throws IOException {
    assertEquals(3, regexQueryNrHits("landau.*", fsa));
  }
}
