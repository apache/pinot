package org.apache.pinot.spi.data;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.FieldSpec.DataType.INT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class SchemaInfoTest {

  @Test
  public void testSchemaInfoSerDeserWithVirtualColumns()
      throws IOException {
    // Mock the Schema object
    Schema schemaMock = mock(Schema.class);
    when(schemaMock.getSchemaName()).thenReturn("TestSchema");
    DimensionFieldSpec dimensionFieldSpec1 = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
    DimensionFieldSpec dimensionFieldSpec2 = new DimensionFieldSpec("dim2", FieldSpec.DataType.INT, true);
    DimensionFieldSpec dimensionFieldSpec3 = new DimensionFieldSpec("dim2", FieldSpec.DataType.INT, true);
    DimensionFieldSpec dimensionFieldSpec4 =
        new DimensionFieldSpec(CommonConstants.Segment.BuiltInVirtualColumn.DOCID, FieldSpec.DataType.INT, true);
    DimensionFieldSpec dimensionFieldSpec5 =
        new DimensionFieldSpec(CommonConstants.Segment.BuiltInVirtualColumn.HOSTNAME, FieldSpec.DataType.STRING, true);
    DimensionFieldSpec dimensionFieldSpec6 =
        new DimensionFieldSpec(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME, FieldSpec.DataType.STRING,
            true);
    List<DimensionFieldSpec> dimensionFieldSpecs = new ArrayList<>();
    dimensionFieldSpecs.add(dimensionFieldSpec1);
    dimensionFieldSpecs.add(dimensionFieldSpec2);
    dimensionFieldSpecs.add(dimensionFieldSpec3);
    dimensionFieldSpecs.add(dimensionFieldSpec4);
    dimensionFieldSpecs.add(dimensionFieldSpec5);
    dimensionFieldSpecs.add(dimensionFieldSpec6);
    when(schemaMock.getDimensionFieldSpecs()).thenReturn(dimensionFieldSpecs);
    DateTimeFieldSpec dateTimeFieldSpec1 =
        new DateTimeFieldSpec("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    DateTimeFieldSpec dateTimeFieldSpec2 =
        new DateTimeFieldSpec("dt2", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    List<DateTimeFieldSpec> dateTimeFieldSpecs = new ArrayList<>();
    dateTimeFieldSpecs.add(dateTimeFieldSpec1);
    dateTimeFieldSpecs.add(dateTimeFieldSpec2);
    when(schemaMock.getDateTimeFieldSpecs()).thenReturn(dateTimeFieldSpecs);
    MetricFieldSpec metricFieldSpec1 = new MetricFieldSpec("metric", INT, -1);
    List<MetricFieldSpec> metricFieldSpecs = new ArrayList<>();
    metricFieldSpecs.add(metricFieldSpec1);
    when(schemaMock.getMetricFieldSpecs()).thenReturn(metricFieldSpecs);
    SchemaInfo schemaInfo = new SchemaInfo(schemaMock);
    List<SchemaInfo> schemaInfoList = new ArrayList<>();
    schemaInfoList.add(schemaInfo);
    String response = JsonUtils.objectToPrettyString(schemaInfoList);
    JsonNode jsonNodeResp = JsonUtils.stringToJsonNode(response);
    assertEquals(jsonNodeResp.get(0).get("schemaName").asText(), "TestSchema");
    assertEquals(jsonNodeResp.get(0).get("numDimensionFields").asInt(), 3);
    assertEquals(jsonNodeResp.get(0).get("numDateTimeFields").asInt(), 2);
    assertEquals(jsonNodeResp.get(0).get("numMetricFields").asInt(), 1);
    assertEquals("TestSchema", schemaInfo.getSchemaName());
    assertEquals(3, schemaInfo.getNumDimensionFields());  // 6 - 3 virtual columns = 3
    assertEquals(2, schemaInfo.getNumDateTimeFields());
    assertEquals(1, schemaInfo.getNumMetricFields());
  }
}
