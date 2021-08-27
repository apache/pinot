package org.apache.pinot.segment.local.utils.nativefst.org.apache.pinot.fsa;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.FSA;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.builders.FSA5Serializer;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.builders.FSABuilder;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.org.apache.pinot.fsa.FSATestUtils.convertToBytes;
import static org.apache.pinot.segment.local.utils.nativefst.org.apache.pinot.fsa.FSATestUtils.regexQueryNrHits;
import static org.testng.Assert.assertEquals;


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

    File directory = new File("./src/test/resources/data/cocacorpus/");

    System.out.println(Paths.get(".").toAbsolutePath().normalize().toString());

    int count1 = 0;

    for (final File fileEntry : directory.listFiles()) {
      fileInputStream = new FileInputStream(fileEntry);
      inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
      bufferedReader = new BufferedReader(inputStreamReader);

      String currentLine;
      while ((currentLine = bufferedReader.readLine()) != null) {
        String[] tmp = currentLine.split("\\s+");    //Split space
        for (String currentWord : tmp) {
          inputStrings.add(currentWord);
          count1 = count1 + currentWord.length();
        }
      }
    }

    System.out.println("Total char1 " + count1);

    inputData = convertToBytes(inputStrings);

    Arrays.sort(inputData, FSABuilder.LEXICAL_ORDERING);
  }

  @BeforeMethod
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

    /*if (initialized != true) {
      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        fos.write(fsaData);
        initialized = true;
      }
    }*/
  }

  @Test
  public void testRegex1() throws IOException {
    assertEquals(207, regexQueryNrHits("q.[aeiou]c.*", fsa));
  }

  @Test
  public void testRegex3() throws IOException {
    assertEquals(20858, regexQueryNrHits("b.*", fsa));
  }

  @Test
  public void testRegex4() throws IOException {
    assertEquals(1204514, regexQueryNrHits(".*", fsa));
  }

  @Test
  public void testRegex5() throws IOException {
    assertEquals(91006, regexQueryNrHits(".*a", fsa));
  }

  @Test
  public void testRandomWords() throws IOException {
    assertEquals(1, regexQueryNrHits("respuestas", fsa));
    assertEquals(1, regexQueryNrHits("Berge", fsa));
    assertEquals(1, regexQueryNrHits("\\@qwx198595", fsa));
    assertEquals(1, regexQueryNrHits("popular", fsa));
    assertEquals(1, regexQueryNrHits("Montella", fsa));
    assertEquals(1, regexQueryNrHits("notably", fsa));
    assertEquals(1, regexQueryNrHits("accepted", fsa));
    assertEquals(1, regexQueryNrHits("challenging", fsa));
    assertEquals(1, regexQueryNrHits("insurance", fsa));
    assertEquals(1, regexQueryNrHits("Calls", fsa));
    assertEquals(1, regexQueryNrHits("certified", fsa));
    assertEquals(1, regexQueryNrHits(".*196169", fsa));
    assertEquals(4299, regexQueryNrHits(".*wx.*", fsa));
    assertEquals(1, regexQueryNrHits("keeps", fsa));
    assertEquals(1, regexQueryNrHits("\\@qwx160430", fsa));
    assertEquals(1, regexQueryNrHits("called", fsa));
    assertEquals(1, regexQueryNrHits("Rid", fsa));
    assertEquals(1, regexQueryNrHits("Computer", fsa));
    assertEquals(1, regexQueryNrHits("\\@qwx871194", fsa));
    assertEquals(1, regexQueryNrHits("control", fsa));
    assertEquals(1, regexQueryNrHits("Gassy", fsa));
    assertEquals(1, regexQueryNrHits("Nut", fsa));
    assertEquals(1, regexQueryNrHits("Strangle", fsa));
    assertEquals(1, regexQueryNrHits("ANYTHING", fsa));
    assertEquals(1, regexQueryNrHits("RiverMusic", fsa));
    assertEquals(1, regexQueryNrHits("\\@qwx420154", fsa));
  }
}
