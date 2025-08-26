/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.response.mapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.apache.pinot.tsdb.spi.series.TimeSeriesException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class TimeSeriesResponseMapperTest {

  @Mock TimeSeriesBlock _block;
  @Mock TimeBuckets _timeBuckets;

  @BeforeTest
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void toBrokerResponseNullBlockThrows() {
    _block = null;
    TimeSeriesResponseMapper.toBrokerResponse(_block);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void toBrokerResponseNonBucketedThrows() {
    when(_block.getTimeBuckets()).thenReturn(null);
    TimeSeriesResponseMapper.toBrokerResponse(_block);
  }

  @Test
  public void toBrokerResponseWithTimeSeriesException() {
    BrokerResponseNativeV2 resp = (BrokerResponseNativeV2) TimeSeriesResponseMapper.toBrokerResponse(
      new TimeSeriesException(QueryErrorCode.INTERNAL, "time series exception"));
    List<QueryProcessingException> exceptions = resp.getExceptions();
    assertEquals(exceptions.size(), 1);
    assertEquals(exceptions.get(0).getErrorCode(), QueryErrorCode.INTERNAL.getId());
  }

  @Test
  public void toBrokerResponseWithEmptySeriesEmptyRowsBaseSchema() {
    when(_block.getSeriesMap()).thenReturn(Collections.emptyMap());
    when(_timeBuckets.getTimeBuckets()).thenReturn(new Long[]{100L, 200L, 300L});
    when(_block.getTimeBuckets()).thenReturn(_timeBuckets);

    BrokerResponseNativeV2 resp = (BrokerResponseNativeV2) TimeSeriesResponseMapper.toBrokerResponse(_block);
    ResultTable table = resp.getResultTable();
    assertNotNull(table);

    DataSchema schema = table.getDataSchema();
    assertNotNull(schema);
    assertEqualsNoOrder(schema.getColumnNames(), new String[]{"ts", "values", "__name__"});
    assertEquals(schema.getColumnDataTypes()[0], DataSchema.ColumnDataType.LONG_ARRAY);
    assertEquals(schema.getColumnDataTypes()[1], DataSchema.ColumnDataType.DOUBLE_ARRAY);
    assertEquals(schema.getColumnDataTypes()[2], DataSchema.ColumnDataType.STRING);

    assertTrue(table.getRows().isEmpty(), "Expected no rows for empty seriesMap");
  }

  @Test
  public void toBrokerResponseForSchemaAndTagsOnlyFromFirstSeries() {
    when(_timeBuckets.getTimeBuckets()).thenReturn(new Long[]{10L, 20L});
    when(_block.getTimeBuckets()).thenReturn(_timeBuckets);
    // first series decides tag columns: region, host
    TimeSeries s1 = new TimeSeries(
      "id1",
      null,
      _timeBuckets,
      new Double[]{1.0, 2.0},
      Arrays.asList("region", "host"),
      new Object[]{"us-west", "h1"}
    );
    // another list under same metric with different extra tag "zone"
    TimeSeries s2 = new TimeSeries(
      "id2",
      null,
      _timeBuckets,
      new Double[]{3.0, 4.5}, // mixed types: Integer and String
      Arrays.asList("region", "zone"),
      new Object[]{"us-west", "z1"}
    );

    Map<Long, List<TimeSeries>> seriesMap = new LinkedHashMap<>();
    seriesMap.put(123456789L, Arrays.asList(s1, s2));
    when(_block.getSeriesMap()).thenReturn(seriesMap);

    BrokerResponseNativeV2 resp = (BrokerResponseNativeV2) TimeSeriesResponseMapper.toBrokerResponse(_block);
    ResultTable table = resp.getResultTable();
    DataSchema schema = table.getDataSchema();

    // schema should include base + first-series tags (region, host), NOT "zone"
    List<String> schemaColumns = Arrays.asList(schema.getColumnNames());
    assertEqualsNoOrder(schemaColumns, Arrays.asList("ts", "values", "__name__", "region", "host"));
    int regionIndex = schemaColumns.indexOf("region");
    int hostIndex = schemaColumns.indexOf("host");

    // verify rows: two rows, tag values present per series
    List<Object[]> rows = table.getRows();
    assertEquals(rows.size(), 2);

    // time buckets
    Object[] r1 = rows.get(0);
    assertTrue(r1[0] instanceof Long[]);
    assertEquals(((Long[]) r1[0]).length, 2);

    assertEquals(r1[1], new Double[]{1.0, 2.0});
    // __name__
    assertEquals(r1[2], "region=us-west,host=h1");
    // region & host
    assertEquals(r1[regionIndex], "us-west");
    assertEquals(r1[hostIndex], "h1");

    // Row 2 (s2)
    Object[] r2 = rows.get(1);
    // values conversion covers Integer and String -> Double
    assertEquals(r2[1], new Double[]{3.0, 4.5});
    // __name__
    assertEquals(r2[2], "region=us-west,zone=z1");
    // region present, host missing (null), "zone" absent from schema
    assertEquals(r2[regionIndex], "us-west");
    assertNull(r2[hostIndex]);
  }
}
