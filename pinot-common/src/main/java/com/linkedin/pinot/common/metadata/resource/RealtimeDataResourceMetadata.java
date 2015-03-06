package com.linkedin.pinot.common.metadata.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metadata.stream.StreamMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.StreamType;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.common.utils.StringUtil;


public class RealtimeDataResourceMetadata {

  private String _resourceName;
  private StreamType _streamType;
  private Schema _dataSchema;
  private StreamMetadata _streamMetadata;
  private ResourceType _resourceType = ResourceType.REALTIME;
  private List<String> _tableList = new ArrayList<String>();
  private String _timeColumnName;
  private String _timeType;
  private int _numDataInstances;
  private int _numDataReplicas;
  private TimeUnit _retentionTimeUnit;
  private int _retentionTimeValue;
  private String _pushFrequency;
  private String _segmentAssignmentStrategy;
  private String _brokerTag;
  private int _NumBrokerInstance;
  private Map<String, String> _metadata;

  public RealtimeDataResourceMetadata() {
  }

  public RealtimeDataResourceMetadata(ZNRecord znRecord) {
    _resourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(znRecord.getSimpleField(CommonConstants.Helix.DataSource.RESOURCE_NAME));
    _streamType =
        StreamType.valueOf(znRecord.getSimpleField(StringUtil.join(".", CommonConstants.Helix.DataSource.METADATA, CommonConstants.Helix.DataSource.Realtime.STREAM_TYPE))
            .toLowerCase());
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

  public String getResourceName() {
    return _resourceName;
  }

  public void setResourceName(String resourceName) {
    _resourceName = resourceName;
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

  public ResourceType getResourceType() {
    return _resourceType;
  }

  public void setResourceType(ResourceType resourceType) {
    _resourceType = resourceType;
  }

  public List<String> getTableList() {
    return _tableList;
  }

  public void setTableList(List<String> tableList) {
    _tableList = tableList;
  }

  public String getTimeColumnName() {
    return _timeColumnName;
  }

  public void setTimeColumnName(String timeColumnName) {
    _timeColumnName = timeColumnName;
  }

  public String getTimeType() {
    return _timeType;
  }

  public void setTimeType(String timeType) {
    _timeType = timeType;
  }

  public int getNumDataInstances() {
    return _numDataInstances;
  }

  public void setNumDataInstances(int numDataInstances) {
    _numDataInstances = numDataInstances;
  }

  public int getNumDataReplicas() {
    return _numDataReplicas;
  }

  public void setNumDataReplicas(int numDataReplicas) {
    _numDataReplicas = numDataReplicas;
  }

  public TimeUnit getRetentionTimeUnit() {
    return _retentionTimeUnit;
  }

  public void setRetentionTimeUnit(TimeUnit retentionTimeUnit) {
    _retentionTimeUnit = retentionTimeUnit;
  }

  public int getRetentionTimeValue() {
    return _retentionTimeValue;
  }

  public void setRetentionTimeValue(int retentionTimeValue) {
    _retentionTimeValue = retentionTimeValue;
  }

  public String getPushFrequency() {
    return _pushFrequency;
  }

  public void setPushFrequency(String pushFrequency) {
    _pushFrequency = pushFrequency;
  }

  public String getSegmentAssignmentStrategy() {
    return _segmentAssignmentStrategy;
  }

  public void setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
    _segmentAssignmentStrategy = segmentAssignmentStrategy;
  }

  public String getBrokerTag() {
    return _brokerTag;
  }

  public void setBrokerTag(String brokerTag) {
    _brokerTag = brokerTag;
  }

  public int getNumBrokerInstance() {
    return _NumBrokerInstance;
  }

  public void setNumBrokerInstance(int numBrokerInstance) {
    _NumBrokerInstance = numBrokerInstance;
  }

  public Map<String, String> getMetadata() {
    return _metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    _metadata = metadata;
  }

  public ZNRecord toZnRecord() {
    ZNRecord znRecord = new ZNRecord(_resourceName);
    znRecord.setSimpleField(CommonConstants.Helix.DataSource.RESOURCE_NAME, _resourceName.substring(0, _resourceName.length() - 2));
    znRecord.setSimpleField(CommonConstants.Helix.DataSource.Realtime.STREAM_TYPE, _streamType.toString());
    znRecord.setSimpleFields(_dataSchema.toMap());
    znRecord.setSimpleFields(_streamMetadata.toMap());
    return znRecord;
  }

  public boolean equals(RealtimeDataResourceMetadata anotherMetadata) {
    if (!anotherMetadata.getResourceName().equals(_resourceName)) {
      return false;
    }
    if (anotherMetadata.getStreamType() != _streamType) {
      return false;
    }
    if (!anotherMetadata.getDataSchema().toString().equals(_dataSchema.toString())) {
      return false;
    }
    if (!anotherMetadata.getStreamMetadata().toString().equals(_streamMetadata.toString())) {
      return false;
    }
    return true;
  }

  public static RealtimeDataResourceMetadata fromZNRecord(ZNRecord record) {
    return new RealtimeDataResourceMetadata(record);
  }
}
