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
package org.apache.pinot.core.query.executor.sql;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.minion.MinionClient;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatement;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatementParser;
import org.apache.pinot.sql.parsers.dml.InsertIntoValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SqlQueryExecutor executes all SQL queries including DQL, DML, DCL, DDL.
 *
 */
public class SqlQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlQueryExecutor.class);

  private final String _controllerUrl;
  private final HelixManager _helixManager;
  private final InsertExecutor _insertExecutor;

  /**
   * Fetch the lead controller from helix, HA is not guaranteed.
   * @param helixManager is used to query leader controller from helix.
   */
  public SqlQueryExecutor(HelixManager helixManager) {
    this(null, helixManager, null);
  }

  /**
   * Recommended to provide the controller vip or service name for access.
   * @param controllerUrl controller service name for sending minion task requests
   */
  public SqlQueryExecutor(String controllerUrl) {
    this(controllerUrl, null, null);
  }

  /**
   * Constructor for controller-local use: the coordinator is wired at construction time,
   * so push-based INSERTs bypass the HTTP round-trip and remain observable on the same process.
   */
  public SqlQueryExecutor(String controllerUrl, @Nullable InsertExecutor insertExecutor) {
    this(controllerUrl, null, insertExecutor);
  }

  private SqlQueryExecutor(@Nullable String controllerUrl, @Nullable HelixManager helixManager,
      @Nullable InsertExecutor insertExecutor) {
    _controllerUrl = controllerUrl;
    _helixManager = helixManager;
    _insertExecutor = insertExecutor;
  }

  private static String getControllerBaseUrl(HelixManager helixManager) {
    String instanceId = LeadControllerUtils.getHelixClusterLeader(helixManager);
    if (instanceId == null) {
      throw new RuntimeException("Unable to locate the leader pinot controller, please retry later...");
    }

    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
    ExtraInstanceConfig extraInstanceConfig = new ExtraInstanceConfig(helixDataAccessor.getProperty(
        keyBuilder.instanceConfig(CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE + instanceId)));
    String controllerBaseUrl = extraInstanceConfig.getComponentUrl();
    if (controllerBaseUrl == null) {
      throw new RuntimeException("Unable to extract the base url from the leader pinot controller");
    }
    return controllerBaseUrl;
  }

  /**
   * Execute DML Statement
   *
   * @param sqlNodeAndOptions Parsed DML object
   * @param headers extra headers map for minion task submission
   * @return BrokerResponse is the DML executed response
   */
  public BrokerResponse executeDMLStatement(SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable Map<String, String> headers) {
    DataManipulationStatement statement = DataManipulationStatementParser.parse(sqlNodeAndOptions);
    BrokerResponseNative result = new BrokerResponseNative();
    switch (statement.getExecutionType()) {
      case MINION:
        AdhocTaskConfig taskConf = statement.generateAdhocTaskConfig();
        try {
          Map<String, String> tableToTaskIdMap = getMinionClient().executeTask(taskConf, headers);
          List<Object[]> rows = new ArrayList<>();
          tableToTaskIdMap.forEach((key, value) -> rows.add(new Object[]{key, value}));
          result.setResultTable(new ResultTable(statement.getResultSchema(), rows));
        } catch (Exception e) {
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
        }
        break;
      case HTTP:
        try {
          result.setResultTable(new ResultTable(statement.getResultSchema(), statement.execute()));
        } catch (Exception e) {
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
        }
        break;
      case PUSH:
        try {
          // InsertResult guarantees non-null state and statementId at construction time (both
          // @JsonCreator and Builder.build() throw on null), so .name() below is safe.
          InsertResult insertResult = executePushInsert(statement, headers);
          List<Object[]> pushRows = new ArrayList<>();
          pushRows.add(new Object[]{
              insertResult.getStatementId(),
              insertResult.getState().name(),
              insertResult.getMessage()
          });
          result.setResultTable(new ResultTable(statement.getResultSchema(), pushRows));
        } catch (IllegalArgumentException e) {
          // Pre-acceptance validation errors (table not found, hybrid-table without explicit type,
          // missing rows, etc.) thrown directly by the in-process path — surface as SQL-parsing-
          // level errors so the broker's HTTP layer maps them to 4xx rather than the generic 5xx-
          // equivalent. Provide a non-empty message even when e.getMessage() is null so clients see
          // something actionable.
          String msg = e.getMessage() != null ? e.getMessage()
              : "Invalid INSERT statement: " + e.getClass().getSimpleName();
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_VALIDATION, msg));
        } catch (JsonMappingException e) {
          // Wire-path malformed response from the controller. Jackson wraps construction-time
          // checks (e.g., null state, null statementId) into JsonMappingException here. Tag with
          // a recognizable prefix so logs/dashboards can distinguish wire-decoding failures from
          // executor failures.
          String msg = "Malformed coordinator response: "
              + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, msg));
        } catch (Exception e) {
          String msg = e.getMessage() != null ? e.getMessage()
              : "INSERT execution failed: " + e.getClass().getSimpleName();
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, msg));
        }
        break;
      default:
        result.addException(
            new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, "Unsupported statement: " + statement));
        break;
    }
    return result;
  }

  private InsertResult executePushInsert(DataManipulationStatement statement,
      @Nullable Map<String, String> headers)
      throws Exception {
    if (!(statement instanceof InsertIntoValues)) {
      throw new IllegalStateException("PUSH execution type requires InsertIntoValues statement");
    }
    InsertIntoValues insertStmt = (InsertIntoValues) statement;

    InsertRequest request = buildInsertRequest(insertStmt);

    // If a local executor is available (controller-side), call it directly
    if (_insertExecutor != null) {
      LOGGER.info("Executing push-based INSERT locally via InsertExecutor");
      return _insertExecutor.execute(request);
    }

    // Otherwise, POST to controller /insert/execute (broker-side). Controller requires
    // ?tableName=... as a query parameter for table-scoped @Authorize binding, so include it.
    String controllerBaseUrl = getControllerUrl();
    String tableName = request.getTableName();
    if (tableName == null || tableName.isEmpty()) {
      throw new RuntimeException("INSERT request missing tableName; cannot route to controller");
    }
    String url = controllerBaseUrl + "/insert/execute?tableName="
        + URLEncoder.encode(tableName, StandardCharsets.UTF_8);
    String payload = JsonUtils.objectToString(request);

    LOGGER.info("Submitting push-based INSERT to controller: {}", url);

    // Forward caller-supplied headers (Authorization in particular). The controller's
    // /insert/execute is @Authenticate(AccessType.CREATE) + @Authorize(action=EXECUTE_INSERT) and
    // rejects unauthenticated requests with 401, so a null headers map fails closed at the
    // controller — but log a warning so misconfigured callers (forgot to plumb headers through
    // the broker dispatch) get a visible signal rather than a generic 401.
    //
    // Strip hop-by-hop and length-bearing headers from the inbound request before forwarding —
    // re-emitting "Content-Length" or "Transfer-Encoding" causes Apache HttpClient to throw
    // "Content-Length header already present" because sendJsonPostRequest computes its own length
    // from the new payload. The forwarded body has nothing to do with the inbound body's framing.
    // Use a case-insensitive map for HTTP headers per RFC 9110 §5.1. A plain HashMap would let
    // inbound "content-type" coexist with our explicit "Content-Type" if a future filter change
    // dropped a strip rule; case-insensitive comparison guarantees our explicit headers win and
    // there are no duplicate-cased entries on the wire.
    Map<String, String> requestHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    requestHeaders.put("Content-Type", "application/json");
    requestHeaders.put("accept", "application/json");
    // Note: the inbound map is single-valued per header name — duplicate-cased inbound headers
    // (e.g. two differently-cased Authorization values) were already collapsed last-value-wins by
    // the caller that built it. Acceptable here since only simple single-valued headers matter.
    if (headers != null && !headers.isEmpty()) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        String name = entry.getKey();
        if (name == null || isStrippedForwardedHeader(name)) {
          continue;
        }
        requestHeaders.put(name, entry.getValue());
      }
    } else {
      LOGGER.warn("executePushInsert: caller-supplied headers are null/empty; the controller will "
          + "reject this request if authentication is enabled. Plumb auth headers through the "
          + "broker dispatch path.");
    }

    String responseStr = HttpClient.wrapAndThrowHttpException(
        HttpClient.getInstance().sendJsonPostRequest(URI.create(url), payload, requestHeaders)).getResponse();
    if (responseStr == null || responseStr.isEmpty()) {
      throw new RuntimeException("Controller /insert/execute returned empty response body for URL: " + url);
    }
    JsonNode responseJson;
    try {
      responseJson = JsonUtils.stringToJsonNode(responseStr);
    } catch (Exception e) {
      // Non-JSON response (HTML error page from a proxy, plaintext from a misconfigured controller,
      // etc.). Surface a clear diagnostic instead of letting Jackson's stack trace bubble.
      throw new RuntimeException("Controller /insert/execute returned non-JSON response. URL=" + url
          + ", first-200-chars=" + responseStr.substring(0, Math.min(200, responseStr.length())), e);
    }
    return JsonUtils.jsonNodeToObject(responseJson, InsertResult.class);
  }

  /**
   * Returns true for headers that must NOT be forwarded from the inbound request to the
   * controller-bound POST. Length/framing-bearing headers ({@code Content-Length},
   * {@code Transfer-Encoding}) cause Apache HttpClient to throw "Content-Length header already
   * present" because the outbound client recomputes them from the new payload. {@code Host} also
   * mismatches the controller URL. {@code Content-Type} and {@code Accept} are explicitly set by
   * us to {@code application/json}; allowing inbound versions through would clobber the JSON
   * contract.
   */
  private static boolean isStrippedForwardedHeader(String name) {
    String lower = name.toLowerCase(Locale.ROOT);
    return lower.equals("content-length")
        || lower.equals("transfer-encoding")
        || lower.equals("content-type")
        || lower.equals("accept")
        || lower.equals("host")
        || lower.equals("connection")
        || lower.equals("expect");
  }

  private static InsertRequest buildInsertRequest(InsertIntoValues insertStmt) {
    List<GenericRow> genericRows = new ArrayList<>();
    List<String> columns = insertStmt.getColumns();
    for (List<Object> rowValues : insertStmt.getRows()) {
      GenericRow genericRow = new GenericRow();
      for (int i = 0; i < columns.size(); i++) {
        Object value = rowValues.get(i);
        if (value == null) {
          // Mark the field as null so that downstream segment generation uses the default null
          // value and records the null in the null bitmap, instead of throwing on a raw null.
          genericRow.putValue(columns.get(i), null);
          genericRow.addNullValueField(columns.get(i));
        } else {
          genericRow.putValue(columns.get(i), value);
        }
      }
      genericRows.add(genericRow);
    }

    TableType tableType = null;
    String tableTypeStr = insertStmt.getTableType();
    if (tableTypeStr != null) {
      tableType = TableType.valueOf(tableTypeStr.toUpperCase(Locale.ROOT));
    }

    // Compute a stable payload hash for idempotency when requestId is set.
    // Exclude control options (requestId, tableType) from the hash so the payload hash
    // only covers table + columns + values + non-control options.
    Map<String, String> payloadOptions = null;
    if (insertStmt.getOptions() != null) {
      payloadOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      payloadOptions.putAll(insertStmt.getOptions());
      payloadOptions.remove("requestId");
      payloadOptions.remove("tableType");
    }
    // Hash is only used for idempotency conflict detection paired with requestId. When the client
    // didn't supply a requestId the coordinator's idempotency path never reads it, so skip the
    // O(rows × columns) SHA-256 computation entirely.
    String payloadHash = insertStmt.getRequestId() != null
        ? computePayloadHash(insertStmt.getTableName(), columns, insertStmt.getRows(), payloadOptions)
        : null;

    return new InsertRequest.Builder()
        .setTableName(insertStmt.getTableName())
        .setTableType(tableType)
        .setInsertType(InsertType.ROW)
        .setRows(genericRows)
        .setRequestId(insertStmt.getRequestId())
        .setPayloadHash(payloadHash)
        .setOptions(insertStmt.getOptions())
        .build();
  }

  /**
   * Computes a stable SHA-256 hash from the table name, column list, row values, and non-control options.
   * Control options like requestId and tableType are excluded so the hash only covers the data payload.
   * This ensures that identical INSERT statements produce the same hash for idempotency.
   */
  private static String computePayloadHash(String tableName, List<String> columns,
      List<List<Object>> rows, Map<String, String> options) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(tableName.getBytes(StandardCharsets.UTF_8));
      digest.update((byte) 0);

      for (String col : columns) {
        digest.update(col.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
      }
      digest.update((byte) 1);

      for (List<Object> row : rows) {
        for (Object val : row) {
          // Coerce to a canonical encoding so the same logical value hashes identically across
          // SDKs that box numerics differently. Type-dispatch keeps the hot path light:
          // integral types take the longValue path (no BigDecimal allocation), only true
          // BigDecimal inputs pay the canonical-form cost. Without canonicalization, two clients
          // sending the same INSERT through different bindings get different hashes and lose
          // idempotency.
          if (val == null) {
            digest.update((byte) 'N');  // null sentinel
          } else if (val instanceof Long || val instanceof Integer || val instanceof Short || val instanceof Byte) {
            // Canonicalize integral types through BigDecimal so SQL-path retries (parser produces
            // BigDecimal) and programmatic callers (raw Long) hash identically. BigDecimal.valueOf
            // is the cheapest path that avoids the toString round-trip.
            digest.update((byte) 'D');
            digest.update(canonicalizeBigDecimal(BigDecimal.valueOf(((Number) val).longValue()))
                .getBytes(StandardCharsets.UTF_8));
          } else if (val instanceof BigDecimal) {
            digest.update((byte) 'D');
            digest.update(canonicalizeBigDecimal((BigDecimal) val).getBytes(StandardCharsets.UTF_8));
          } else if (val instanceof BigInteger) {
            // Canonicalize through BigDecimal so a BigInteger and an equivalent Long produce the
            // same idempotency hash. Without this, two clients that send the same logical value
            // via different SDK numeric bindings would observe IDEMPOTENCY_CONFLICT on retry.
            digest.update((byte) 'D');
            digest.update(canonicalizeBigDecimal(new BigDecimal((BigInteger) val))
                .getBytes(StandardCharsets.UTF_8));
          } else if (val instanceof Double || val instanceof Float) {
            // NaN/Infinity are valid SQL VALUES but cannot pass through BigDecimal (throws
            // NumberFormatException on "NaN"/"Infinity"). Hash a stable sentinel byte instead so
            // the entire INSERT doesn't crash. Differentiates positive/negative infinity and NaN.
            double d = ((Number) val).doubleValue();
            if (!Double.isFinite(d)) {
              digest.update((byte) 'D');
              if (Double.isNaN(d)) {
                digest.update((byte) 'X');  // X = NaN sentinel
              } else if (d > 0) {
                digest.update((byte) 'P');  // P = +Infinity sentinel
              } else {
                digest.update((byte) 'M');  // M = -Infinity sentinel
              }
            } else {
              // Canonicalize through BigDecimal via the type's own toString so Float and Double of
              // the same user-typed numeric literal hash identically. Float.doubleValue() bakes in
              // the widening artifact (Float 0.1f → 0.10000000149...) and would diverge from
              // Double 0.1 — using the type's canonical string representation avoids that.
              digest.update((byte) 'D');
              String canonical = (val instanceof Float)
                  ? Float.toString((Float) val)
                  : Double.toString((Double) val);
              digest.update(canonicalizeBigDecimal(new BigDecimal(canonical))
                  .getBytes(StandardCharsets.UTF_8));
            }
          } else if (val instanceof Boolean) {
            digest.update((byte) 'B');
            digest.update(((Boolean) val) ? (byte) 1 : (byte) 0);
          } else if (val instanceof byte[]) {
            digest.update((byte) 'X');  // raw bytes
            digest.update((byte[]) val);
          } else {
            digest.update((byte) 'S');  // string-coerced
            digest.update(val.toString().getBytes(StandardCharsets.UTF_8));
          }
          digest.update((byte) 0);
        }
        digest.update((byte) 2);
      }

      if (options != null && !options.isEmpty()) {
        // Sort keys for deterministic ordering using a case-insensitive comparator so callers that
        // pass mixed-case option keys (defensive against upstream parser variation) hash to the
        // same value as a canonical-cased version. Matches the case-insensitive option lookup the
        // upstream parser does in InsertIntoValues.findOptionCaseInsensitive.
        TreeMap<String, String> canonical = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        canonical.putAll(options);
        for (Map.Entry<String, String> entry : canonical.entrySet()) {
          digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) 0);
          digest.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) 0);
        }
      }

      return BytesUtils.toHexString(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }

  /**
   * Canonicalize a {@link BigDecimal} for hashing so equivalent values produce identical strings.
   * Handles the zero edge case explicitly: {@code BigDecimal.ZERO.stripTrailingZeros()} returns
   * {@code 0E-1} (a denormalized form) on JDK 8+ which would diverge from {@code "0"} produced by
   * {@code new BigDecimal("0").toPlainString()}. Special-casing zero forces the canonical form
   * "0" regardless of input scale.
   */
  private static String canonicalizeBigDecimal(BigDecimal bd) {
    if (bd.signum() == 0) {
      return "0";
    }
    return bd.stripTrailingZeros().toPlainString();
  }

  private String getControllerUrl() {
    if (_controllerUrl != null) {
      return _controllerUrl;
    }
    if (_helixManager != null) {
      return getControllerBaseUrl(_helixManager);
    }
    throw new RuntimeException("No controller URL configured");
  }

  private MinionClient getMinionClient() {
    // NOTE: using null auth provider here as auth headers injected by caller in "executeDMLStatement()"
    if (_helixManager != null) {
      return new MinionClient(getControllerBaseUrl(_helixManager), null);
    }
    return new MinionClient(_controllerUrl, null);
  }
}
