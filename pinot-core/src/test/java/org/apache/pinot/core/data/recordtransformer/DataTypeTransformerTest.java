package org.apache.pinot.core.data.recordtransformer;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DataTypeTransformerTest {

  @Test
  public void testDataTypeTransformer() {
    Schema pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("SV1", FieldSpec.DataType.INT)
        .addMultiValueDimension("MV1", FieldSpec.DataType.INT)
        .addMultiValueDimension("MV2", FieldSpec.DataType.STRING)
        .addMultiValueDimension("MV3", FieldSpec.DataType.LONG)
        .addMultiValueDimension("MV4", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("MV5", FieldSpec.DataType.DOUBLE)
        .build();

    // generate test data
    Map<String, String> map1 = new HashMap<>();
    map1.put("item", "10");
    Map<String, String> map2 = new HashMap<>();
    map2.put("item", "20");
    Object[] objectArray = new Object[]{map1, map2};
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("SV1", 1.1);
    genericRow.putDefaultNullValue("MV1", objectArray);
    genericRow.putDefaultNullValue("MV2", objectArray);
    genericRow.putDefaultNullValue("MV3", objectArray);
    genericRow.putDefaultNullValue("MV4", objectArray);
    genericRow.putDefaultNullValue("MV5", objectArray);

    DataTypeTransformer dataTypeTransformer = new DataTypeTransformer(pinotSchema);
    dataTypeTransformer.transform(genericRow);

    assertEquals(genericRow.getValue("SV1"), 1);
    assertEquals(genericRow.getValue("MV1"), new Integer[]{10, 20});
    assertEquals(genericRow.getValue("MV2"), new String[]{"10", "20"});
    assertEquals(genericRow.getValue("MV3"), new Long[]{10L, 20L});
    assertEquals(genericRow.getValue("MV4"), new Float[]{10f, 20f});
    assertEquals(genericRow.getValue("MV5"), new Double[]{10d, 20d});
  }
}
