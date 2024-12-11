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
package org.apache.pinot.query.runtime.timeseries.serde;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


/**
 * Implements a simple Serde mechanism for the Time Series Block. This is used for transferring data between servers
 * and brokers. The approach is to use a {@link TransferableBlock} and rely on the existing serialization code to avoid
 * re-inventing the wheel. Once the time-series engine coalesces with the Multistage Engine, we will anyway use
 * TransferableBlock for data transfers.
 * <p>
 *   The {@link TimeSeriesBlock} is converted to and from a table, where the first row contains information about the
 *   time-buckets. For each tag/label in the query, there's a dedicated column, and the Double values are stored in
 *   the last column. As an example, consider the following, where FBV represents the first bucket value of TimeBuckets.
 *   <pre>
 *     +-------------+------------+-------------+---------------------------------+
 *     | tag-0       | tag-1      | tag-n       | values (bytes[][] or double[])  |
 *     +-------------+------------+-------------+---------------------------------+
 *     | null        | null       | null        | [FBV, bucketSize, numBuckets]   |
 *     +-------------+------------+-------------+---------------------------------+
 *     | Chicago     | 60607      | ...         | [value-0, value-1, ... value-x] |
 *     +-------------+------------+-------------+---------------------------------+
 *     | San Fran.   | 94107      | ...         | [value-0, value-1, ... value-x] |
 *     +-------------+------------+-------------+---------------------------------+
 *   </pre>
 *   TODO(timeseries): When we support Time Series selection queries, we will likely need a special column instead of
 *     tags, because one could store data in JSON Blobs and the series may have different tags/labels.
 * </p>
 * <p>
 *  TODO(timeseries): One source of inefficiency is boxing/unboxing of Double arrays.
 *  TODO(timeseries): The other is tag values being Object[]. We should make tag values String[].
 * </p>
 */
public class TimeSeriesBlockSerde {
  /**
   * Since DataBlock can only handle primitive double[] arrays, we use Double.MIN_VALUE to represent nulls.
   * Using Double.MIN_VALUE is better than using Double.NaN since Double.NaN can help detect divide by 0.
   * TODO(timeseries): Check if we can get rid of boxed Doubles altogether.
   */
  private static final String VALUES_COLUMN_NAME = "__ts_serde_values";
  private static final double NULL_PLACEHOLDER = Double.MIN_VALUE;

  private TimeSeriesBlockSerde() {
  }

  public static TimeSeriesBlock deserializeTimeSeriesBlock(ByteBuffer readOnlyByteBuffer)
      throws IOException {
    DataBlock dataBlock = DataBlockUtils.readFrom(readOnlyByteBuffer);
    TransferableBlock transferableBlock = TransferableBlockUtils.wrap(dataBlock);
    List<String> tagNames = generateTagNames(Objects.requireNonNull(transferableBlock.getDataSchema(),
        "Missing data schema in TransferableBlock"));
    final DataSchema dataSchema = transferableBlock.getDataSchema();
    List<Object[]> container = transferableBlock.getContainer();
    TimeBuckets timeBuckets = timeBucketsFromRow(container.get(0), dataSchema);
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
    for (int index = 1; index < container.size(); index++) {
      Object[] row = container.get(index);
      TimeSeries timeSeries = timeSeriesFromRow(tagNames, row, timeBuckets, dataSchema);
      long seriesId = Long.parseLong(timeSeries.getId());
      seriesMap.computeIfAbsent(seriesId, x -> new ArrayList<>()).add(timeSeries);
    }
    return new TimeSeriesBlock(timeBuckets, seriesMap);
  }

  public static ByteString serializeTimeSeriesBlock(TimeSeriesBlock timeSeriesBlock)
      throws IOException {
    TimeBuckets timeBuckets = Objects.requireNonNull(timeSeriesBlock.getTimeBuckets());
    List<Object[]> container = new ArrayList<>();
    DataSchema dataSchema = generateDataSchema(timeSeriesBlock);
    container.add(timeBucketsToRow(timeBuckets, dataSchema));
    for (var entry : timeSeriesBlock.getSeriesMap().entrySet()) {
      for (TimeSeries timeSeries : entry.getValue()) {
        container.add(timeSeriesToRow(timeSeries, dataSchema));
      }
    }
    TransferableBlock transferableBlock = new TransferableBlock(container, dataSchema, DataBlock.Type.ROW);
    return DataBlockUtils.toByteString(transferableBlock.getDataBlock());
  }

  private static DataSchema generateDataSchema(TimeSeriesBlock timeSeriesBlock) {
    TimeSeries sampledTimeSeries = sampleTimeSeries(timeSeriesBlock).orElse(null);
    int numTags = sampledTimeSeries == null ? 0 : sampledTimeSeries.getTagNames().size();
    ColumnDataType[] dataTypes = new ColumnDataType[numTags + 1];
    final ColumnDataType valueDataType = inferValueDataType(sampledTimeSeries);
    String[] columnNames = new String[numTags + 1];
    for (int tagIndex = 0; tagIndex < numTags; tagIndex++) {
      columnNames[tagIndex] = sampledTimeSeries.getTagNames().get(tagIndex);
      dataTypes[tagIndex] = ColumnDataType.STRING;
    }
    columnNames[numTags] = VALUES_COLUMN_NAME;
    dataTypes[numTags] = valueDataType;
    return new DataSchema(columnNames, dataTypes);
  }

  private static List<String> generateTagNames(DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    List<String> tagNames = new ArrayList<>(columnNames.length - 1);
    for (int index = 0; index < columnNames.length - 1; index++) {
      tagNames.add(columnNames[index]);
    }
    return tagNames;
  }

  private static Optional<TimeSeries> sampleTimeSeries(TimeSeriesBlock timeSeriesBlock) {
    if (timeSeriesBlock.getSeriesMap().isEmpty()) {
      return Optional.empty();
    }
    List<TimeSeries> timeSeriesList = timeSeriesBlock.getSeriesMap().values().iterator().next();
    Preconditions.checkState(!timeSeriesList.isEmpty(), "Found empty time-series list");
    return Optional.of(timeSeriesList.get(0));
  }

  private static ColumnDataType inferValueDataType(@Nullable TimeSeries timeSeries) {
    if (timeSeries == null || timeSeries.getValues() instanceof Double[]) {
      return ColumnDataType.DOUBLE_ARRAY;
    }
    return ColumnDataType.BYTES_ARRAY;
  }

  private static Object[] timeBucketsToRow(TimeBuckets timeBuckets, DataSchema dataSchema) {
    int numColumns = dataSchema.getColumnNames().length;
    Object[] result = new Object[numColumns];
    for (int index = 0; index < numColumns - 1; index++) {
      result[index] = "null";
    }
    double firstBucketValue = timeBuckets.getTimeBuckets()[0];
    double bucketSizeSeconds = timeBuckets.getBucketSize().getSeconds();
    double numBuckets = timeBuckets.getNumBuckets();
    final ColumnDataType valuesDataType = dataSchema.getColumnDataTypes()[numColumns - 1];
    final double[] bucketsEncodedAsDouble = new double[]{firstBucketValue, bucketSizeSeconds, numBuckets};
    if (valuesDataType == ColumnDataType.DOUBLE_ARRAY) {
      result[numColumns - 1] = bucketsEncodedAsDouble;
    } else {
      Preconditions.checkState(valuesDataType == ColumnDataType.BYTES_ARRAY,
          "Expected bytes_array column type. Found: %s", valuesDataType);
      result[numColumns - 1] = toBytesArray(bucketsEncodedAsDouble);
    }
    return result;
  }

  private static TimeBuckets timeBucketsFromRow(Object[] row, DataSchema dataSchema) {
    int numColumns = dataSchema.getColumnDataTypes().length;
    double[] values;
    if (dataSchema.getColumnDataTypes()[numColumns - 1] == ColumnDataType.BYTES_ARRAY) {
      values = fromBytesArray((byte[][]) row[row.length - 1]);
    } else {
      values = (double[]) row[row.length - 1];
    }
    long fbv = (long) values[0];
    Duration window = Duration.ofSeconds((long) values[1]);
    int numBuckets = (int) values[2];
    return TimeBuckets.ofSeconds(fbv, window, numBuckets);
  }

  private static Object[] timeSeriesToRow(TimeSeries timeSeries, DataSchema dataSchema) {
    int numColumns = dataSchema.getColumnNames().length;
    Object[] result = new Object[numColumns];
    for (int index = 0; index < numColumns - 1; index++) {
      Object tagValue = timeSeries.getTagValues()[index];
      result[index] = tagValue == null ? "null" : tagValue.toString();
    }
    if (dataSchema.getColumnDataTypes()[numColumns - 1] == ColumnDataType.DOUBLE_ARRAY) {
      result[numColumns - 1] = unboxDoubleArray(timeSeries.getDoubleValues());
    } else {
      result[numColumns - 1] = timeSeries.getBytesValues();
    }
    return result;
  }

  private static TimeSeries timeSeriesFromRow(List<String> tagNames, Object[] row, TimeBuckets timeBuckets,
      DataSchema dataSchema) {
    int numColumns = dataSchema.getColumnDataTypes().length;
    Object[] tagValues = new Object[row.length - 1];
    System.arraycopy(row, 0, tagValues, 0, row.length - 1);
    Object[] values;
    if (dataSchema.getColumnDataTypes()[numColumns - 1] == ColumnDataType.DOUBLE_ARRAY) {
      values = boxDoubleArray((double[]) row[row.length - 1]);
    } else {
      values = (byte[][]) row[row.length - 1];
    }
    return new TimeSeries(Long.toString(TimeSeries.hash(tagValues)), null, timeBuckets, values, tagNames, tagValues);
  }

  private static byte[][] toBytesArray(double[] values) {
    byte[][] result = new byte[values.length][8];
    for (int index = 0; index < values.length; index++) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(result[index]);
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
      byteBuffer.putDouble(values[index]);
    }
    return result;
  }

  private static double[] fromBytesArray(byte[][] bytes) {
    double[] result = new double[bytes.length];
    for (int index = 0; index < bytes.length; index++) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes[index]);
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
      result[index] = byteBuffer.getDouble();
    }
    return result;
  }

  private static double[] unboxDoubleArray(Double[] values) {
    double[] result = new double[values.length];
    for (int index = 0; index < result.length; index++) {
      result[index] = values[index] == null ? NULL_PLACEHOLDER : values[index];
    }
    return result;
  }

  private static Double[] boxDoubleArray(double[] values) {
    Double[] result = new Double[values.length];
    for (int index = 0; index < result.length; index++) {
      result[index] = values[index] == NULL_PLACEHOLDER ? null : values[index];
    }
    return result;
  }
}
