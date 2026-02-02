package org.apache.pinot.query.service.dispatch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.StreamingBrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;


/// A [org.apache.pinot.common.response.BrokerResponse] that is lazily evaluated.
///
/// This class can only be used once the data schema is known. In case there is an error before that, an
/// early error [EarlyResponse] should be used.
public class LazyBrokerResponse implements StreamingBrokerResponse {
  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(LazyBrokerResponse.class);

  private final DataSchema _dataSchema;
  private final MultiStageOperator _rootOperator;
  @Nullable
  private Metainfo _metainfo;

  public LazyBrokerResponse(DataSchema dataSchema, MultiStageOperator rootOperator) {
    _dataSchema = dataSchema;
    _rootOperator = rootOperator;
  }

  @Override
  public Metainfo getMetaInfo()
      throws IllegalStateException {
    Preconditions.checkState(_metainfo != null, "MetaInfo is not available before consuming data");
    return _metainfo;
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @Override
  public Metainfo consumeData(DataConsumer consumer)
      throws InterruptedException {
    if (_metainfo != null) {
      return _metainfo;
    }
    long start = System.currentTimeMillis();
    MseBlock mseBlock = _rootOperator.nextBlock();
    while (!mseBlock.isEos()) {
      MseBlock.Data mseDataBlock = (MseBlock.Data) mseBlock;
      StreamingBrokerResponse.Data brokerBlock = mseDataBlock.accept(MseBlockDecorator.INSTANCE, _dataSchema);
      consumer.consume(brokerBlock);
      mseBlock = _rootOperator.nextBlock();
    }
    assert mseBlock.isEos();
    _metainfo = createMetainfo((MseBlock.Eos) mseBlock, System.currentTimeMillis() - start);
    return _metainfo;
  }

  private static Metainfo createMetainfo(MseBlock.Eos block, long reduceTimeMs) {
    // TODO: Improve the error handling, e.g. return partial response
    if (block.isError()) {
      ErrorMseBlock errorBlock = (ErrorMseBlock) block;
      Map<QueryErrorCode, String> queryExceptions = errorBlock.getErrorMessages();

      String errorMessage;
      Map.Entry<QueryErrorCode, String> error;
      String from;
      if (errorBlock.getStageId() >= 0) {
        from = " from stage " + errorBlock.getStageId();
        if (errorBlock.getServerId() != null) {
          from += " on " + errorBlock.getServerId();
        }
      } else {
        from = "";
      }
      if (queryExceptions.size() == 1) {
        error = queryExceptions.entrySet().iterator().next();
        errorMessage = "Received 1 error" + from + ": " + error.getValue();
      } else {
        error = queryExceptions.entrySet().stream().max(LazyBrokerResponse::compareErrors).orElseThrow();
        errorMessage =
            "Received " + queryExceptions.size() + " errors" + from + ". " + "The one with highest priority is: "
                + error.getValue();
      }
      return new Metainfo.Error(List.of(new QueryProcessingException(error.getKey().getId(), errorMessage)));
    }
    assert block.isSuccess();

    return new Metainfo() {
      @Override
      public List<QueryProcessingException> getExceptions() {
        return List.of();
      }

      @Override
      public ObjectNode asJson() {
        return JsonUtils.newObjectNode()
            .set("exceptions", JsonUtils.newArrayNode());
      }
    };
  }

  @Override
  public void close() {
    _rootOperator.close();
  }

  // TODO: Improve the way the errors are compared
  private static int compareErrors(
      Map.Entry<QueryErrorCode, String> entry1,
      Map.Entry<QueryErrorCode, String> entry2
  ) {
    QueryErrorCode errorCode1 = entry1.getKey();
    QueryErrorCode errorCode2 = entry2.getKey();
    if (errorCode1 == QueryErrorCode.QUERY_VALIDATION) {
      return 1;
    }
    if (errorCode2 == QueryErrorCode.QUERY_VALIDATION) {
      return -1;
    }
    return Integer.compare(errorCode1.getId(), errorCode2.getId());
  }

  public static class MseBlockDecorator implements MseBlock.Data.Visitor<StreamingBrokerResponse.Data, DataSchema> {
    public static final MseBlockDecorator INSTANCE = new MseBlockDecorator();

    @Override
    public StreamingBrokerResponse.Data visit(RowHeapDataBlock block, DataSchema arg) {
      return new StreamingBrokerResponse.Data.FromObjectArrList(block.getRows());
    }

    @Override
    public StreamingBrokerResponse.Data visit(SerializedDataBlock block, DataSchema arg) {
      DataBlock dataBlock = block.getDataBlock();
      return new StreamingBrokerResponse.Data() {
        private int _currentId = 0;
        @Override
        public int getNumRows() {
          return dataBlock.getNumberOfRows();
        }

        @Override
        public Object get(int colIdx) {
          DataSchema.ColumnDataType columnDataType = arg.getColumnDataType(colIdx);
          Object internal = getInternal(colIdx, columnDataType);
          return columnDataType.toExternal(internal);
        }

        private Object getInternal(int colIdx, DataSchema.ColumnDataType columnDataType) {
          switch (columnDataType.getStoredType()) {
            // TODO: Verify if more types are needed
            case INT:
              return dataBlock.getInt(0, colIdx);
            case LONG:
              return dataBlock.getLong(0, colIdx);
            case FLOAT:
              return dataBlock.getFloat(0, colIdx);
            case DOUBLE:
              return dataBlock.getDouble(0, colIdx);
            case BIG_DECIMAL:
              return dataBlock.getBigDecimal(0, colIdx);
            case STRING:
              return dataBlock.getString(0, colIdx);
            case BYTES:
              return dataBlock.getBytes(0, colIdx);
            case INT_ARRAY:
              return dataBlock.getIntArray(0, colIdx);
            case LONG_ARRAY:
              return dataBlock.getLongArray(0, colIdx);
            case FLOAT_ARRAY:
              return dataBlock.getFloatArray(0, colIdx);
            case DOUBLE_ARRAY:
              return dataBlock.getDoubleArray(0, colIdx);
            case STRING_ARRAY:
              return dataBlock.getStringArray(0, colIdx);
            case MAP:
              return dataBlock.getMap(0, colIdx);
            default:
              throw new UnsupportedOperationException("Column " + arg.getColumnName(colIdx)
                  + " has unsupported storage type " + columnDataType.getStoredType());
          }
        }

        @Override
        public boolean next() {
          return _currentId++ < getNumRows();
        }
      };
    }
  }
}
