package org.apache.pinot.common.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;

/// An adaptor that converts an eager [BrokerResponse] into a lazy [StreamingBrokerResponse].
public class EagerToLazyBrokerResponseAdaptor implements StreamingBrokerResponse {
  private final BrokerResponse _earlyResponse;
  private boolean _dataEmitted = false;

  public EagerToLazyBrokerResponseAdaptor(BrokerResponse earlyResponse) {
    _earlyResponse = earlyResponse;
  }
  @Override
  @Nullable
  public DataSchema getDataSchema() {
    ResultTable resultTable = _earlyResponse.getResultTable();
    if (resultTable == null) {
      return null;
    }
    return resultTable.getDataSchema();
  }

  @Override
  public Metainfo getMetaInfo() {
    return new EagerBrokerResponseToMetainfo(_earlyResponse);
  }

  @Override
  public Metainfo consumeData(DataConsumer consumer)
      throws InterruptedException {
    if (_dataEmitted) {
      return getMetaInfo();
    }

    ResultTable resultTable = _earlyResponse.getResultTable();
    _dataEmitted = true;
    if (resultTable == null) {
      return getMetaInfo();
    }
    StreamingBrokerResponse.Data.FromObjectArrList block = new Data.FromObjectArrList(resultTable.getRows());
    consumer.consume(block);
    return getMetaInfo();
  }

  @Override
  public void close() {
  }

  public static class EagerBrokerResponseToMetainfo implements StreamingBrokerResponse.Metainfo {
    private static final ObjectMapper OBJECT_MAPPER;
    protected final BrokerResponse _brokerResponse;

    static {
      OBJECT_MAPPER = JsonUtils.createMapper();
      OBJECT_MAPPER.addMixIn(BrokerResponse.class, StreamingBrokerMetainfoMixing.class);
    }

    /// Returns the backing eager [BrokerResponse].
    ///
    /// Any modification to the returned object will be reflected in [#getMetaInfo()].
    public BrokerResponse getBrokerResponse() {
      return _brokerResponse;
    }

    public EagerBrokerResponseToMetainfo(BrokerResponse brokerResponse) {
      _brokerResponse = brokerResponse;
    }

    @Override
    public List<QueryProcessingException> getExceptions() {
      return _brokerResponse.getExceptions();
    }

    @Override
    public ObjectNode asJson() {
      return OBJECT_MAPPER.valueToTree(_brokerResponse);
    }
  }

  /// A mixin class to add serialization support for [StreamingBrokerResponse.Metainfo].
  ///
  /// Mixins are a Jackson feature that allows adding annotations to classes without modifying their source code.
  /// In this case we use a mixin to ignore the "resultTable" field in the [BrokerResponse] class when serializing
  /// it as a [StreamingBrokerResponse.Metainfo], since the result table is not part of the metainfo.
  ///
  /// The two other alternatives are:
  /// * To serialize the metainfo manually, which is more error-prone and less maintainable.
  /// + To serialize the response as a whole and then remove the result table from the serialized JSON,
  ///   which is less efficient.
  private interface StreamingBrokerMetainfoMixing {
    @JsonIgnore
    ResultTable getResultTable();
  }
}
