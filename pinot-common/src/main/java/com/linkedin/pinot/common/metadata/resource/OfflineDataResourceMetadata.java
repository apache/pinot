package com.linkedin.pinot.common.metadata.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;


public class OfflineDataResourceMetadata {
  private String _resourceName;
  private Schema _dataSchema;
  private ResourceType _resourceType = ResourceType.OFFLINE;
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

  public OfflineDataResourceMetadata() {
  }

  public OfflineDataResourceMetadata(ZNRecord record) {
    // TODO Auto-generated constructor stub
  }

  public static OfflineDataResourceMetadata fromZNRecord(ZNRecord record) {
    return new OfflineDataResourceMetadata(record);
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
  
}
