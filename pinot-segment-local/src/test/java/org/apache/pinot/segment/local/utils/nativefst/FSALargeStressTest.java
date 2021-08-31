package org.apache.pinot.segment.local.utils.nativefst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.utils.fst.FSTBuilder;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSABuilder;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.listEqualsIgnoreOrder;
import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHits;
import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHitsWithResults;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class FSALargeStressTest {
  private static FSA nativeFST;
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

    nativeFST = FSABuilder.buildFSA(inputStrings);
    fst = FSTBuilder.buildFST(inputStrings);
  }

  @Test
  public void testRegex1() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("q.[aeiou]c.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("q.[aeiou]c.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex2() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*ba.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*ba.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex3() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("b.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("b.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex5() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*a", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*a", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRandomWords() throws IOException {
    assertEquals(1, regexQueryNrHits("respuestas", nativeFST));
    assertEquals(1, regexQueryNrHits("Berge", nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx198595", nativeFST));
    assertEquals(1, regexQueryNrHits("popular", nativeFST));
    assertEquals(1, regexQueryNrHits("Montella", nativeFST));
    assertEquals(1, regexQueryNrHits("notably", nativeFST));
    assertEquals(1, regexQueryNrHits("accepted", nativeFST));
    assertEquals(1, regexQueryNrHits("challenging", nativeFST));
    assertEquals(1, regexQueryNrHits("insurance", nativeFST));
    assertEquals(1, regexQueryNrHits("Calls", nativeFST));
    assertEquals(1, regexQueryNrHits("certified", nativeFST));
    assertEquals(1, regexQueryNrHits(".*196169", nativeFST));
    assertEquals(4290, regexQueryNrHits(".*wx.*", nativeFST));
    assertEquals(1, regexQueryNrHits("keeps", nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx160430", nativeFST));
    assertEquals(1, regexQueryNrHits("called", nativeFST));
    assertEquals(1, regexQueryNrHits("Rid", nativeFST));
    assertEquals(1, regexQueryNrHits("Computer", nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx871194", nativeFST));
    assertEquals(1, regexQueryNrHits("control", nativeFST));
    assertEquals(1, regexQueryNrHits("Gassy", nativeFST));
    assertEquals(1, regexQueryNrHits("Nut", nativeFST));
    assertEquals(1, regexQueryNrHits("Strangle", nativeFST));
    assertEquals(1, regexQueryNrHits("ANYTHING", nativeFST));
    assertEquals(1, regexQueryNrHits("RiverMusic", nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx420154", nativeFST));
  }
}
