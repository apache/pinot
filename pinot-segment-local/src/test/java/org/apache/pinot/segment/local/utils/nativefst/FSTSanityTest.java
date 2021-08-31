package org.apache.pinot.segment.local.utils.nativefst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.utils.fst.FSTBuilder;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSABuilder;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.listEqualsIgnoreOrder;
import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHitsWithResults;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class FSTSanityTest {
  private FSA nativeFST;
  private FST<Long> fst;

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
    int i = 0;
    while((currentWord = bufferedReader.readLine()) != null) {
      inputStrings.put(currentWord, i);
      i++;
    }

    nativeFST = FSABuilder.buildFSA(inputStrings);
    fst = FSTBuilder.buildFST(inputStrings);
  }

  @Test
  public void testRegex1() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("q.[aeiou]c.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("q.[aeiou]c.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex2() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("a.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("a.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex3() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("b.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("b.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex4() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*", nativeFST);

    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex5() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*landau", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*landau", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex6() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("landau.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("landau.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }


  @Test
  public void testRegex7() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*ated", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*ated", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex8() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*ed", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*ed", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex9() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*pot.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*pot.*", nativeFST);

    System.out.println("length is " + results.size() + " " + nativeResults.size());
    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }
}
