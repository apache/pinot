package org.apache.pinot.segment.local.utils.nativefst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.utils.fst.FSTBuilder;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSABuilder;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHits;
import static org.testng.Assert.assertEquals;


public class FSALargeStressTest {
  private static FSA fsa;
  private static FST<Long> fst;

  @BeforeClass
  public static void setUp() throws Exception {
    SortedMap<String, Integer> inputStrings = new TreeMap<>();
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
          inputStrings.put(currentWord, (int) Math.random());
          count1 = count1 + currentWord.length();
        }
      }
    }

    fsa = FSABuilder.buildFSA(inputStrings);
    fst = FSTBuilder.buildFST(inputStrings);
  }

  @Test
  public void testRegex1() throws IOException {
    System.out.println(RegexpMatcher.regexMatch("q.[aeiou]c.*", fst).size() + " " + regexQueryNrHits("q.[aeiou]c.*", fsa));
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
    System.out.println(RegexpMatcher.regexMatch(".*a", fst).size() + " " + regexQueryNrHits(".*a", fsa));
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
