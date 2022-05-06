package org.apache.pinot.plugin.inputformat.csv;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CSVMessageDecoderTest {

  @Test
  public void testHappyCase()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, Set.of("name", "age", "gender"), "");
    String incomingRecord = "Alice;18;F";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
  }

  @Test
  public void testMultivalue()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("csvHeader", "name;age;gender;subjects");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, Set.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "Alice;18;F;maths,German,history";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));
    Assert.assertNotNull(destination.getValue("subjects"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
    Assert.assertEquals(destination.getValue("subjects"), new String[]{"maths", "German", "history"});
  }

  @Test
  public void testCommentMarker()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("csvHeader", "name,age,gender");
    decoderProps.put("csvDelimiter", ",");
    decoderProps.put("csvCommentMarker", "#");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, Set.of("name", "age", "gender"), "");
    String incomingRecord = "#Alice,18,F";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);

    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");

  }

  @Test
  public void testHeaderFromRecord()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.remove("csvHeader");
    decoderProps.put("csvDelimiter", ",");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, Set.of("name", "age", "gender"), "");
    String incomingRecord = "name,age,gender\nAlice,18,F";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);

    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
  }

  @Test
  public void testEscapeCharacter()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("csvHeader", "name;age;gender;subjects");
    decoderProps.put("csvDelimiter", ";");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, Set.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "Alice;18;F;mat\\;hs";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));
    Assert.assertNotNull(destination.getValue("subjects"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
    Assert.assertEquals(destination.getValue("subjects"), "mat;hs");
  }

  private static Map<String, String> getStandardDecoderProps() {
    //setup
    Map<String, String> props = new HashMap<>();
    props.put("csvHeader", "name;age;gender");
    props.put("csvDelimiter", ",");
    props.put("csvMultiValueDelimiter", ",");
    props.put("csvEscapeCharacter", "\\");
    return props;
  }
}
