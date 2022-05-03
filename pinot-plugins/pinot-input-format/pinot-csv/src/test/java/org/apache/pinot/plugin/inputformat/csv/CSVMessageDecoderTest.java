package org.apache.pinot.plugin.inputformat.csv;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;
import org.testng.collections.Sets;


public class CSVMessageDecoderTest {

  @Test
  public void testHappyCase()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, Set.of("name,age,gender,subjects"), "");
    String incomingRecord = "Alice,18,F,maths;history;German";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
  }

  private static Map<String, String> getStandardDecoderProps() {
    //setup
    Map<String, String> props = new HashMap<>();
    props.put("csvHeader", "name,age,gender,subjects");
    props.put("csvDelimiter", ";");
    props.put("csvMultiValueDelimiter", ",");
    return props;
  }
}
