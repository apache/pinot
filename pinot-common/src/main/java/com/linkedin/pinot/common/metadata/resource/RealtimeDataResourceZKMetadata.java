package com.linkedin.pinot.common.metadata.resource;

import java.util.Map;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metadata.stream.StreamMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.StreamType;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;


public class RealtimeDataResourceZKMetadata extends DataResourceZKMetadata {

  private StreamType _streamType;
  private Schema _dataSchema;
  private StreamMetadata _streamMetadata;

  public RealtimeDataResourceZKMetadata() {
    setResourceType(ResourceType.REALTIME);
  }

  public RealtimeDataResourceZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    setResourceType(ResourceType.REALTIME);
    _streamType = znRecord.getEnumField(CommonConstants.Helix.DataSource.Realtime.STREAM_TYPE, StreamType.class, null);
    _dataSchema = Schema.getSchemaFromMap(znRecord.getSimpleFields());
    switch (_streamType) {
      case kafka:
        _streamMetadata = new KafkaStreamMetadata(znRecord.getSimpleFields());
        break;
      default:
        throw new UnsupportedOperationException("Not support stream type - " + _streamType);
    }
  }

  public StreamType getStreamType() {
    return _streamType;
  }

  public void setStreamType(StreamType streamType) {
    _streamType = streamType;
  }

  public Schema getDataSchema() {
    return _dataSchema;
  }

  public void setDataSchema(Schema dataSchema) {
    _dataSchema = dataSchema;
  }

  public StreamMetadata getStreamMetadata() {
    return _streamMetadata;
  }

  public void setStreamMetadata(StreamMetadata streamMetadata) {
    _streamMetadata = streamMetadata;
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(BrokerRequestUtils.getRealtimeResourceNameForResource(getResourceName()));
    Map<String, String> fieldMap = _dataSchema.toMap();
    fieldMap.putAll(_streamMetadata.toMap());
    znRecord.setSimpleFields(fieldMap);
    znRecord.merge(super.toZNRecord());
    znRecord.setSimpleField(CommonConstants.Helix.DataSource.Realtime.STREAM_TYPE, _streamType.toString());
    return znRecord;
  }

  public boolean equals(RealtimeDataResourceZKMetadata anotherMetadata) {
    if (!super.equals(anotherMetadata)) {
      return false;
    }
    if (anotherMetadata.getStreamType() != _streamType ||
        !anotherMetadata.getDataSchema().toString().equals(_dataSchema.toString()) ||
        !anotherMetadata.getStreamMetadata().toString().equals(_streamMetadata.toString())) {
      return false;
    }
    return true;
  }

  public static RealtimeDataResourceZKMetadata fromZNRecord(ZNRecord record) {
    return new RealtimeDataResourceZKMetadata(record);
  }

}
