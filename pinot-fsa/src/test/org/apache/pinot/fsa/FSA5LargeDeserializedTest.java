package org.apache.pinot.fsa;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.fsa.builders.FSABuilder;
import org.apache.pinot.fsa.utils.RegexpMatcher;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FSA5LargeDeserializedTest extends TestBase {
  private FSA fsa;

  @Before
  public void setUp() throws Exception {
    InputStream fileInputStream = null;
    File file = new File("./src/test/resources/large_fsa_serialized.txt");

    fileInputStream = new FileInputStream(file);

    fsa = FSA.read(fileInputStream, true);
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
}
