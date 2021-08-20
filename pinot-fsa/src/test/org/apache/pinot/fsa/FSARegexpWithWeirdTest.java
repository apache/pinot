package org.apache.pinot.fsa;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.fsa.builders.FSA5Serializer;
import org.apache.pinot.fsa.builders.FSABuilder;
import org.apache.pinot.fsa.utils.RegexpMatcher;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class FSARegexpWithWeirdTest extends TestBase {
  private FSA fsa;

  @Before
  public void setUp()
      throws IOException {
    String regexTestInputString =
        "@qwx196169";
    String[] splitArray = regexTestInputString.split("\\s+");
    byte[][] bytesArray = convertToBytes(splitArray);

    Arrays.sort(bytesArray, FSABuilder.LEXICAL_ORDERING);

    FSABuilder fsaBuilder = new FSABuilder();

    for (byte[] currentArray : bytesArray) {
      fsaBuilder.add(currentArray, 0, currentArray.length, -1);
    }

    FSA s = fsaBuilder.complete();

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(s, new ByteArrayOutputStream())
            .toByteArray();

    fsa = FSA.read(new ByteArrayInputStream(fsaData),
        FSA5.class, true);

    System.out.println(fsa.toString());
  }

  @Test
  public void testRegex1() throws IOException {
    assertEquals(1, regexQueryNrHits(".*196169"));
  }

  /**
   * Return all matches for given regex
   */
  private long regexQueryNrHits(String regex) throws IOException {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, fsa);

    return resultList.size();
  }

  private static byte[][] convertToBytes(String[] strings) {
    byte[][] data = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
      String string = strings[i];
      data[i] = string.getBytes(Charset.defaultCharset()); // you can chose charset
    }
    return data;
  }
}
