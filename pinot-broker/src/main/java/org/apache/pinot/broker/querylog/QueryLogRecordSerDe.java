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
package org.apache.pinot.broker.querylog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * Binary serializer/deserializer for {@link QueryLogRecord}. Records are compressed individually so they can be
 * appended to arbitrary storage backends.
 */
final class QueryLogRecordSerDe {
  private QueryLogRecordSerDe() {
  }

  static byte[] serialize(QueryLogRecord record)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(baos);
        DataOutputStream out = new DataOutputStream(gzip)) {
      out.writeLong(record.getLogTimestampMs());
      out.writeLong(record.getRequestId());
      writeString(out, record.getTableName());
      writeString(out, record.getBrokerId());
      writeString(out, record.getClientIp());
      writeString(out, record.getQuery());
      writeString(out, record.getQueryEngine());
      out.writeLong(record.getRequestArrivalTimeMs());
      out.writeLong(record.getTimeMs());
      out.writeLong(record.getBrokerReduceTimeMs());
      out.writeLong(record.getNumDocsScanned());
      out.writeLong(record.getTotalDocs());
      out.writeLong(record.getNumEntriesScannedInFilter());
      out.writeLong(record.getNumEntriesScannedPostFilter());
      out.writeLong(record.getNumSegmentsQueried());
      out.writeLong(record.getNumSegmentsProcessed());
      out.writeLong(record.getNumSegmentsMatched());
      out.writeLong(record.getNumConsumingSegmentsQueried());
      out.writeLong(record.getNumConsumingSegmentsProcessed());
      out.writeLong(record.getNumConsumingSegmentsMatched());
      out.writeLong(record.getNumUnavailableSegments());
      out.writeLong(record.getMinConsumingFreshnessTimeMs());
      out.writeInt(record.getNumServersResponded());
      out.writeInt(record.getNumServersQueried());
      out.writeBoolean(record.isGroupsTrimmed());
      out.writeBoolean(record.isGroupLimitReached());
      out.writeBoolean(record.isGroupWarningLimitReached());
      out.writeLong(record.getNumExceptions());
      writeString(out, record.getExceptions());
      writeString(out, record.getServerStats());
      out.writeLong(record.getOfflineTotalCpuTimeNs());
      out.writeLong(record.getOfflineThreadCpuTimeNs());
      out.writeLong(record.getOfflineSystemActivitiesCpuTimeNs());
      out.writeLong(record.getOfflineResponseSerializationCpuTimeNs());
      out.writeLong(record.getRealtimeTotalCpuTimeNs());
      out.writeLong(record.getRealtimeThreadCpuTimeNs());
      out.writeLong(record.getRealtimeSystemActivitiesCpuTimeNs());
      out.writeLong(record.getRealtimeResponseSerializationCpuTimeNs());
      out.writeLong(record.getOfflineTotalMemAllocatedBytes());
      out.writeLong(record.getOfflineThreadMemAllocatedBytes());
      out.writeLong(record.getOfflineResponseSerMemAllocatedBytes());
      out.writeLong(record.getRealtimeTotalMemAllocatedBytes());
      out.writeLong(record.getRealtimeThreadMemAllocatedBytes());
      out.writeLong(record.getRealtimeResponseSerMemAllocatedBytes());
      writeIntArray(out, record.getPools());
      out.writeBoolean(record.isPartialResult());
      out.writeBoolean(record.isRlsFiltersApplied());
      out.writeInt(record.getNumRowsResultSet());
      writeStringArray(out, record.getTablesQueried());
      writeString(out, record.getTraceInfoJson());
      writeString(out, record.getFanoutType());
      writeString(out, record.getOfflineServerTenant());
      writeString(out, record.getRealtimeServerTenant());
    }
    return baos.toByteArray();
  }

  static QueryLogRecord deserialize(byte[] payload)
      throws IOException {
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(payload));
        DataInputStream in = new DataInputStream(gzip)) {
      long logTimestampMs = in.readLong();
      long requestId = in.readLong();
      String tableName = readString(in);
      String brokerId = readString(in);
      String clientIp = readString(in);
      String query = readString(in);
      String queryEngine = readString(in);
      long requestArrivalTimeMs = in.readLong();
      long timeMs = in.readLong();
      long brokerReduceTimeMs = in.readLong();
      long numDocsScanned = in.readLong();
      long totalDocs = in.readLong();
      long numEntriesScannedInFilter = in.readLong();
      long numEntriesScannedPostFilter = in.readLong();
      long numSegmentsQueried = in.readLong();
      long numSegmentsProcessed = in.readLong();
      long numSegmentsMatched = in.readLong();
      long numConsumingSegmentsQueried = in.readLong();
      long numConsumingSegmentsProcessed = in.readLong();
      long numConsumingSegmentsMatched = in.readLong();
      long numUnavailableSegments = in.readLong();
      long minConsumingFreshnessTimeMs = in.readLong();
      int numServersResponded = in.readInt();
      int numServersQueried = in.readInt();
      boolean groupsTrimmed = in.readBoolean();
      boolean groupLimitReached = in.readBoolean();
      boolean groupWarningLimitReached = in.readBoolean();
      long numExceptions = in.readLong();
      String exceptions = readString(in);
      String serverStats = readString(in);
      long offlineTotalCpuTimeNs = in.readLong();
      long offlineThreadCpuTimeNs = in.readLong();
      long offlineSystemActivitiesCpuTimeNs = in.readLong();
      long offlineResponseSerializationCpuTimeNs = in.readLong();
      long realtimeTotalCpuTimeNs = in.readLong();
      long realtimeThreadCpuTimeNs = in.readLong();
      long realtimeSystemActivitiesCpuTimeNs = in.readLong();
      long realtimeResponseSerializationCpuTimeNs = in.readLong();
      long offlineTotalMemAllocatedBytes = in.readLong();
      long offlineThreadMemAllocatedBytes = in.readLong();
      long offlineResponseSerMemAllocatedBytes = in.readLong();
      long realtimeTotalMemAllocatedBytes = in.readLong();
      long realtimeThreadMemAllocatedBytes = in.readLong();
      long realtimeResponseSerMemAllocatedBytes = in.readLong();
      int[] pools = readIntArray(in);
      boolean partialResult = in.readBoolean();
      boolean rlsFiltersApplied = in.readBoolean();
      int numRowsResultSet = in.readInt();
      String[] tablesQueried = readStringArray(in);
      String traceInfoJson = readString(in);
      String fanoutType = readString(in);
      String offlineServerTenant = readString(in);
      String realtimeServerTenant = readString(in);

      return new QueryLogRecord(logTimestampMs, requestId, tableName, brokerId, clientIp, query, queryEngine,
          requestArrivalTimeMs, timeMs, brokerReduceTimeMs, numDocsScanned, totalDocs, numEntriesScannedInFilter,
          numEntriesScannedPostFilter, numSegmentsQueried, numSegmentsProcessed, numSegmentsMatched,
          numConsumingSegmentsQueried, numConsumingSegmentsProcessed, numConsumingSegmentsMatched,
          numUnavailableSegments, minConsumingFreshnessTimeMs, numServersResponded, numServersQueried, groupsTrimmed,
          groupLimitReached, groupWarningLimitReached, numExceptions, exceptions, serverStats, offlineTotalCpuTimeNs,
          offlineThreadCpuTimeNs, offlineSystemActivitiesCpuTimeNs, offlineResponseSerializationCpuTimeNs,
          realtimeTotalCpuTimeNs, realtimeThreadCpuTimeNs, realtimeSystemActivitiesCpuTimeNs,
          realtimeResponseSerializationCpuTimeNs, offlineTotalMemAllocatedBytes, offlineThreadMemAllocatedBytes,
          offlineResponseSerMemAllocatedBytes, realtimeTotalMemAllocatedBytes, realtimeThreadMemAllocatedBytes,
          realtimeResponseSerMemAllocatedBytes, pools, partialResult, rlsFiltersApplied, numRowsResultSet,
          tablesQueried, traceInfoJson, fanoutType, offlineServerTenant, realtimeServerTenant);
    }
  }

  private static void writeString(DataOutputStream out, String value)
      throws IOException {
    if (value == null) {
      out.writeBoolean(false);
      return;
    }
    out.writeBoolean(true);
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInputStream in)
      throws IOException {
    boolean present = in.readBoolean();
    if (!present) {
      return null;
    }
    int length = in.readInt();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static void writeStringArray(DataOutputStream out, String[] values)
      throws IOException {
    if (values == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(values.length);
    for (String value : values) {
      writeString(out, value);
    }
  }

  private static String[] readStringArray(DataInputStream in)
      throws IOException {
    int length = in.readInt();
    if (length < 0) {
      return null;
    }
    String[] values = new String[length];
    for (int i = 0; i < length; i++) {
      values[i] = readString(in);
    }
    return values;
  }

  private static void writeIntArray(DataOutputStream out, int[] values)
      throws IOException {
    if (values == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(values.length);
    for (int value : values) {
      out.writeInt(value);
    }
  }

  private static int[] readIntArray(DataInputStream in)
      throws IOException {
    int length = in.readInt();
    if (length < 0) {
      return null;
    }
    int[] values = new int[length];
    for (int i = 0; i < length; i++) {
      values[i] = in.readInt();
    }
    return values;
  }
}
