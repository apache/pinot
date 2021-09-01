package org.apache.pinot.segment.local.utils.nativefst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSABuilder;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHits;
import static org.testng.Assert.assertEquals;

public class FSASmallStressTest {
  private FSA fsa;

  @BeforeTest
  public void setUp() throws Exception {
    SortedMap<String, Integer> inputStrings = new TreeMap<>();
    InputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;

    File file = new File("./src/test/resources/data/words.txt");

    fileInputStream = new FileInputStream(file);
    inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
    bufferedReader = new BufferedReader(inputStreamReader);

    String currentWord;
    while((currentWord = bufferedReader.readLine()) != null) {
      inputStrings.put(currentWord, (int) Math.random());
    }

    fsa = FSABuilder.buildFSA(inputStrings);

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
    assertEquals(24370, regexQueryNrHits(".*a", fsa));
  }

  @Test
  public void testRegex3() throws IOException {
    assertEquals(18969, regexQueryNrHits("b.*", fsa));
  }

  @Test
  public void testRegex4() throws IOException {
    assertEquals(466550, regexQueryNrHits(".*", fsa));
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
