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
package org.apache.pinot.common.response;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;

/// A streaming broker response that allows consuming data blocks one by one.
///
/// This is used by the broker whenever it can stream data to the client as soon as it is available rather than
/// buffering the full response in memory. This is especially useful for MSE queries that return large amounts of data.
///
/// It is important to note that the [#consumeData] method can only be called once and may be called by a thread where
/// [org.apache.pinot.spi.query.QueryThreadContext] may not be set up. Therefore, implementations should be careful
/// to not rely on thread-local context during data consumption.
public interface StreamingBrokerResponse extends AutoCloseable {
  @Nullable
  DataSchema getDataSchema();

  /// Consumes data blocks by passing them to the given consumer until a terminal block is reached.
  ///
  /// This method can only be called once.
  /// @throws IllegalStateException If called more than once.
  Metainfo consumeData(DataConsumer consumer)
      throws InterruptedException, IllegalStateException;

  /// Returns the metadata after consuming all data blocks.
  ///
  /// @throws IllegalStateException If called before calling [#consumeData].
  Metainfo getMetaInfo()
      throws IllegalStateException;

  default BrokerResponse asEagerBrokerResponse() {
    return LazyToEagerBrokerResponseAdaptor.of(this);
  }

  @Override
  void close();

  default StreamingBrokerResponse withDecoratedMetainfo(Consumer<ObjectNode> decorator) {
    return new MetainfoJsonDecorator(this) {
      @Override
      protected ObjectNode decorateMetainfoJson(ObjectNode json) {
        decorator.accept(json);
        return json;
      }
    };
  }

  default StreamingBrokerResponse withPostMetainfo(Consumer<StatMap<StdMetaField>> postUpdater) {
    return new StdMetainfoDecorator(this).withPostMetainfo(postUpdater);
  }


  default StreamingBrokerResponse withListener(BiConsumer<StreamingBrokerResponse, Metainfo> onConsumptionFinished) {
    return new WithListener(this, onConsumptionFinished);
  }

  static StreamingBrokerResponse error(QueryErrorCode errorCode) {
    return error(errorCode, null);
  }

  static StreamingBrokerResponse error(QueryErrorCode errorCode, @Nullable String errorMessage) {
    String actualErrorMessage = errorMessage != null ? errorMessage : errorCode.getDefaultMessage();
    QueryProcessingException processingException = new QueryProcessingException(errorCode.getId(), actualErrorMessage);

    return new EarlyResponse(new Metainfo.Error(List.of(processingException)));
  }

  /// The request information obtained after consuming all data blocks.
  ///
  /// Implementations must be serializable to JSON using [JsonUtils].
  interface Metainfo {

    List<QueryProcessingException> getExceptions();

    @Nullable
    default QueryErrorCode getErrorCode() {
      List<QueryProcessingException> processingExceptions = getExceptions();
      return processingExceptions != null && !processingExceptions.isEmpty()
          ? QueryErrorCode.fromErrorCode(processingExceptions.get(0).getErrorCode())
          : null;
    }

    @Nullable
    default String getErrorMessage() {
      return getExceptions() != null && !getExceptions().isEmpty()
          ? getExceptions().get(0).getMessage()
          : null;
    }

    /// Serializes the metainfo to a JSON object.
    ///
    /// The returned JSON object can be further modified before being sent to the client.
    ObjectNode asJson();

    default void emitBrokerResponseMetrics(BrokerMetrics brokerMetrics) {
      for (QueryProcessingException exception : getExceptions()) {
        QueryErrorCode queryErrorCode;
        try {
          queryErrorCode = QueryErrorCode.fromErrorCode(exception.getErrorCode());
        } catch (IllegalArgumentException e) {
          queryErrorCode = QueryErrorCode.UNKNOWN;
        }
        brokerMetrics.addMeteredGlobalValue(BrokerMeter.getQueryErrorMeter(queryErrorCode), 1);
      }
    }

    class Error implements Metainfo {
      private final List<QueryProcessingException> _processingExceptions;

      public Error(QueryErrorCode errorCode, String errorMessage) {
        this(List.of(new QueryProcessingException(errorCode.getId(), errorMessage)));
      }

      public Error(List<QueryProcessingException> processingExceptions) {
        _processingExceptions = processingExceptions;
      }

      @Override
      public List<QueryProcessingException> getExceptions() {
        return _processingExceptions;
      }

      @Override
      public ObjectNode asJson() {
        return JsonUtils.newObjectNode()
            .set("exceptions", JsonUtils.objectToJsonNode(_processingExceptions));
      }
    }

    class Delegator implements Metainfo {
      private final Metainfo _delegate;

      public Delegator(Metainfo delegate) {
        _delegate = delegate;
      }

      @Override
      public List<QueryProcessingException> getExceptions() {
        return _delegate.getExceptions();
      }

      @Override
      public ObjectNode asJson() {
        return _delegate.asJson();
      }
    }

    class PostDecorator implements Metainfo {
      private final Metainfo _delegate;
      private final Function<ObjectNode, ObjectNode> _postUpdater;

      public PostDecorator(Metainfo delegate, Function<ObjectNode, ObjectNode> postUpdater) {
        _delegate = delegate;
        _postUpdater = postUpdater;
      }

      @Override
      public List<QueryProcessingException> getExceptions() {
        return _delegate.getExceptions();
      }

      @Override
      public ObjectNode asJson() {
        ObjectNode json = _delegate.asJson();
        return _postUpdater.apply(json);
      }
    }
  }

  interface DataConsumer {
    void consume(Data data)
        throws InterruptedException;
  }

  interface Data {
    int getNumRows();

    /// Returns the value at the given column index for the current row.
    ///
    /// The returned value must be compatible with the
    /// [external type of the column](org.apache.pinot.common.utils.DataSchema.ColumnDataType#toExternal(Object))
    @Nullable
    Object get(int colIdx);

    boolean next();

    /// A [Data] implementation that wraps a list of object arrays.
    ///
    /// It is assumed that each object array represents a row, and each element in the array represents a column value,
    /// in [external formal][org.apache.pinot.common.utils.DataSchema.ColumnDataType#toExternal(Object)].
    /// In case your rows are in internal format, use [FromObjectArrList#fromInternal] to create a [Data] that
    /// returns values in internal format.
    class FromObjectArrList implements Data {
      private final List<Object[]> _rows;
      private int _currentId = 0;

      /// Creates a new [FromObjectArrList] with the given rows.
      ///
      /// It is assumed that each object array represents a row, and each element in the array represents a column value,
      /// in [external formal][org.apache.pinot.common.utils.DataSchema.ColumnDataType#toExternal(Object)].
      /// In case your rows are in internal format, use [FromObjectArrList#fromInternal] to create a [Data] that
      /// returns values in internal format.
      public FromObjectArrList(List<Object[]> rows) {
        _rows = rows;
      }

      public static FromObjectArrList fromInternal(DataSchema schema, List<Object[]> rows) {
        return fromInternal(schema.getColumnDataTypes(), rows);
      }

      public static FromObjectArrList fromInternal(DataSchema.ColumnDataType[] colTypes, List<Object[]> rows) {
        return new FromObjectArrList(rows) {
          @Override
          public Object get(int colIdx) {
            Object value = super.get(colIdx);
            if (value == null) {
              return null;
            }
            return colTypes[colIdx].toExternal(value);
          }
        };
      }

      @Override
      public int getNumRows() {
        return _rows.size();
      }

      @Override
      public Object get(int colIdx) {
        Preconditions.checkState(_currentId > 0,
            "Cannot get value for row %s before calling next()", _currentId);
        Preconditions.checkState(_currentId <= getNumRows(),
            "Cannot get value for row %s after reaching end of stream", _currentId);
        return _rows.get(_currentId - 1)[colIdx];
      }

      @Override
      public boolean next() {
        return _currentId++ < getNumRows();
      }
    }
  }

  /// A streaming broker response that contains no data and immediately returns a [Metainfo].
  ///
  /// Usually, this is used when an error occurs before the query could be planned or parsed.
  class EarlyResponse implements StreamingBrokerResponse {
    private final Metainfo _metainfo;

    public EarlyResponse(Metainfo metainfo) {
      _metainfo = metainfo;
    }

    @Nullable
    @Override
    public DataSchema getDataSchema() {
      return null;
    }

    @Override
    public Metainfo consumeData(DataConsumer consumer)
        throws InterruptedException {
      return _metainfo;
    }

    @Override
    public Metainfo getMetaInfo()
        throws IllegalStateException {
      return _metainfo;
    }

    @Override
    public void close() {
    }
  }

  abstract class Delegator implements StreamingBrokerResponse {
    protected final StreamingBrokerResponse _delegate;

    public Delegator(StreamingBrokerResponse delegate) {
      _delegate = delegate;
    }

    @Nullable
    @Override
    public DataSchema getDataSchema() {
      return _delegate.getDataSchema();
    }

    @Override
    public Metainfo consumeData(DataConsumer consumer)
        throws InterruptedException {
      return _delegate.consumeData(consumer);
    }

    @Override
    public Metainfo getMetaInfo() {
      return _delegate.getMetaInfo();
    }

    @Override
    public BrokerResponse asEagerBrokerResponse() {
      return _delegate.asEagerBrokerResponse();
    }

    @Override
    public void close() {
      _delegate.close();
    }
  }

  class WithListener extends StreamingBrokerResponse.Delegator {
    private final List<BiConsumer<StreamingBrokerResponse, Metainfo>> _listeners;

    public WithListener(
        StreamingBrokerResponse delegate,
        BiConsumer<StreamingBrokerResponse, Metainfo> onConsumptionFinished
    ) {
      super(delegate);
      _listeners = new ArrayList<>(1);
      _listeners.add(onConsumptionFinished);
    }

    @Override
    public Metainfo consumeData(DataConsumer consumer)
        throws InterruptedException {
      Metainfo metainfo = _delegate.consumeData(consumer);
      for (BiConsumer<StreamingBrokerResponse, Metainfo> listener : _listeners) {
        listener.accept(this, metainfo);
      }
      return metainfo;
    }

    @Override
    public StreamingBrokerResponse withListener(BiConsumer<StreamingBrokerResponse, Metainfo> onConsumptionFinished) {
      _listeners.add(onConsumptionFinished);
      return this;
    }
  }

  class StdMetainfoDecorator extends MetainfoJsonDecorator {
    private final StatMap<StdMetaField> _stats = new StatMap<>(StdMetaField.class);
    private final List<Consumer<StatMap<StdMetaField>>> _postUpdaters = new ArrayList<>();

    public StdMetainfoDecorator(StreamingBrokerResponse delegate) {
      super(delegate);
    }

    @Override
    public StreamingBrokerResponse withPostMetainfo(Consumer<StatMap<StdMetaField>> postUpdater) {
      _postUpdaters.add(postUpdater);
      return this;
    }

    @Override
    protected ObjectNode decorateMetainfoJson(ObjectNode json) {
      for (Consumer<StatMap<StdMetaField>> postUpdater : _postUpdaters) {
        postUpdater.accept(_stats);
      }

      ObjectNode statsJson = _stats.asJson();

      statsJson.forEachEntry(json::set);

      return statsJson;
    }
  }

  abstract class MetainfoJsonDecorator extends StreamingBrokerResponse.Delegator {

    public MetainfoJsonDecorator(StreamingBrokerResponse delegate) {
      super(delegate);
    }

    protected abstract ObjectNode decorateMetainfoJson(ObjectNode json);

    @Override
    public Metainfo consumeData(DataConsumer consumer)
        throws InterruptedException {
      Metainfo metainfo = _delegate.consumeData(consumer);
      return new Metainfo.PostDecorator(metainfo, this::decorateMetainfoJson);
    }
  }

  enum StdMetaField implements StatMap.Key {
    NUM_DOCS_SCANNED(StatMap.Type.LONG),
    TOTAL_DOCS(StatMap.Type.LONG),
    NUM_ENTRIES_SCANNED_IN_FILTER(StatMap.Type.LONG),
    NUM_ENTRIES_SCANNED_POST_FILTER(StatMap.Type.LONG),
    NUM_SEGMENTS_QUERIED(StatMap.Type.INT),
    NUM_SEGMENTS_PROCESSED(StatMap.Type.INT),
    NUM_SEGMENTS_MATCHED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_QUERIED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_PROCESSED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_MATCHED(StatMap.Type.INT),
    MIN_CONSUMING_FRESHNESS_TIME_MS(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return StatMap.Key.minPositive(value1, value2);
      }
    },
    NUM_SEGMENTS_PRUNED_BY_SERVER(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_INVALID(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_LIMIT(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_VALUE(StatMap.Type.INT),
    GROUPS_TRIMMED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_WARNING_LIMIT_REACHED(StatMap.Type.BOOLEAN),

    MAX_ROWS_IN_JOIN_REACHED(StatMap.Type.BOOLEAN),
    MAX_ROWS_IN_WINDOW_REACHED(StatMap.Type.BOOLEAN),

    CLIENT_REQUEST_ID(StatMap.Type.STRING),
    BROKER_REDUCE_TIME_MS(StatMap.Type.LONG),

    NUM_SERVERS_QUERIED(StatMap.Type.INT),
    NUM_SERVERS_RESPONDED(StatMap.Type.INT),;

    private final StatMap.Type _type;

    StdMetaField(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }

  class ListStreamingBrokerResponse implements StreamingBrokerResponse {
    private final DataSchema _dataSchema;
    private final Metainfo _metainfo;
    private final List<Object[]> _rows;

    public ListStreamingBrokerResponse(DataSchema dataSchema, Metainfo metainfo, List<Object[]> rows) {
      _dataSchema = dataSchema;
      _metainfo = metainfo;
      _rows = rows;
    }

    @Override
    public @Nullable DataSchema getDataSchema() {
      return _dataSchema;
    }

    @Override
    public Metainfo consumeData(DataConsumer consumer)
        throws InterruptedException, IllegalStateException {
      Data.FromObjectArrList block = new Data.FromObjectArrList(_rows);
      consumer.consume(block);
      return _metainfo;
    }

    @Override
    public Metainfo getMetaInfo()
        throws IllegalStateException {
      return _metainfo;
    }

    @Override
    public void close() {
    }
  }
}
