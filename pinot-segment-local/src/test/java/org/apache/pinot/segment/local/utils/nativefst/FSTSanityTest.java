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

import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHits;
import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHitsWithResults;
import static org.testng.Assert.assertEquals;


public class FSTSanityTest {
  private FSA nativeFST;
  private FST<Long> fst;

  @BeforeTest
  public void setUp() throws Exception {
    SortedMap<String, Integer> inputStrings = new TreeMap<>();
    InputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;

    File file = new File("./src/test/resources/data/wordsbar.txt");

    fileInputStream = new FileInputStream(file);
    inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
    bufferedReader = new BufferedReader(inputStreamReader);

    String currentWord;
    int i = 0;
    while((currentWord = bufferedReader.readLine()) != null) {
      //TODO: atri
      System.out.println("WORD IS " + currentWord + " ID IS " + i);
      inputStrings.put(currentWord, i);
      i++;
    }

    nativeFST = FSABuilder.buildFSA(inputStrings);
    fst = FSTBuilder.buildFST(inputStrings);
  }

  @Test
  public void testRegex1() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("q.[aeiou]c.*", fst);
    long nativeResult = regexQueryNrHits("q.[aeiou]c.*", nativeFST);

    assertEquals(results.size(), nativeResult);
  }

  @Test
  public void testRegex2() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("a.*", fst);
    long nativeResult = regexQueryNrHits("a.*", nativeFST);

    assertEquals(results.size(), nativeResult);
  }

  @Test
  public void testRegex3() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("b.*", fst);
    long nativeResult = regexQueryNrHits("b.*", nativeFST);

    assertEquals(results.size(), nativeResult);
  }

  @Test
  public void testRegex4() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*", fst);
    long nativeResult = regexQueryNrHits(".*", nativeFST);

    assertEquals(results.size(), nativeResult);
  }

  @Test
  public void testRegex7() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*ated", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*ated", nativeFST);

    System.out.println("FIRST LIST IS " + nativeResults);
    System.out.println("SECOND LIST IS " + results);

    System.out.println(results.size() + " " + nativeResults.size());

    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex5() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*landau", fst);
    long nativeResult = regexQueryNrHits(".*landau", nativeFST);

    System.out.println("foo is " + nativeResult + " result val is " + results.size());

    assertEquals(results.size(), nativeResult);
  }

  @Test
  public void testRegex6() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("landau.*", fst);
    long nativeResult = regexQueryNrHits("landau.*", nativeFST);

    assertEquals(results.size(), nativeResult);
  }
}
