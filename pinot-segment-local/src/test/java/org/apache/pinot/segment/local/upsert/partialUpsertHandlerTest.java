package org.apache.pinot.segment.local.upsert;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class partialUpsertHandlerTest {

  @Test
  public void testMerge() {
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    String tableNameWithType = "testTable_Realtime";
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.INCREMENT);
    PartialUpsertHandler handler = new PartialUpsertHandler(helixManager, tableNameWithType, partialUpsertStrategies);

    // both records are null.
    GenericRow previousRecord = new GenericRow();
    GenericRow incomingRecord = new GenericRow();

    previousRecord.putDefaultNullValue("field1", 0);
    incomingRecord.putDefaultNullValue("field1", 0);
    GenericRow newRecord = handler.merge(previousRecord, incomingRecord);
    assertEquals(newRecord.isNullValue("field1"), true);
    assertEquals(newRecord.getValue("field1"), 0);

    // previousRecord is null default value, while newRecord is not.
    previousRecord = new GenericRow();
    incomingRecord = new GenericRow();
    previousRecord.putDefaultNullValue("field1", 0);
    incomingRecord.putValue("field1", 2);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertEquals(newRecord.isNullValue("field1"), false);
    assertEquals(newRecord.getValue("field1"), 2);

    // newRecord is default null value, while previousRecord is not.
    previousRecord = new GenericRow();
    incomingRecord = new GenericRow();
    previousRecord.putValue("field1", 1);
    incomingRecord.putDefaultNullValue("field1", 0);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertEquals(newRecord.isNullValue("field1"), false);
    assertEquals(newRecord.getValue("field1"), 1);

    // neither of records is null.
    previousRecord = new GenericRow();
    incomingRecord = new GenericRow();
    previousRecord.putValue("field1", 1);
    incomingRecord.putValue("field1", 2);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertEquals(newRecord.isNullValue("field1"), false);
    assertEquals(newRecord.getValue("field1"), 3);
  }
}
