package com.linkedin.pinot.core.realtime.kafka;

import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.MutableIndexSegment;
import com.linkedin.pinot.core.realtime.RealtimeIndexingConfig;
import com.linkedin.pinot.core.realtime.RealtimeSegment;
import com.linkedin.pinot.core.realtime.RealtimeSegmentDataManager;
import com.linkedin.pinot.core.realtime.SegmentMetadataChangeContext;
import com.linkedin.pinot.core.realtime.SegmentMetadataListener;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;

/**
 * @author kgopalak
 */
public class KafkaRealtimeSegmentDataManager implements RealtimeSegmentDataManager {

  RealtimeSegment realtimeSegment;
  private StreamProviderConfig streamProviderConfig;
  private KafkaStreamProvider kafkaStreamProvider;
  boolean stop;
  private String resourceName;
  private String segmentName;
  private KafkaSegmentMetadataChangeListener segmentMetadataChangeListener;
  private HelixManager helixManager;
  String metadataPathinZK;
  final private ZkHelixPropertyStore<ZNRecord> helixPropertyStore;
  private SegmentMetadataListenerInvoker invoker;

  public KafkaRealtimeSegmentDataManager(HelixManager manager, String resourceName,
      String segmentName) {
    this.helixManager = manager;
    this.resourceName = resourceName;
    this.segmentName = segmentName;
    this.metadataPathinZK = calculateSegmentMetadataPath();
    this.helixPropertyStore = manager.getHelixPropertyStore();

  }

  @Override
  public void init(RealtimeIndexingConfig realtimeIndexingConfig) {
    realtimeSegment = createRealtimeSegment(realtimeIndexingConfig);
    streamProviderConfig = getStreamProviderConfig(realtimeIndexingConfig);
    kafkaStreamProvider = getStreamProvider(streamProviderConfig);
    segmentMetadataChangeListener = createMetadataChangeListener();
    invoker = new SegmentMetadataListenerInvoker(helixManager, segmentMetadataChangeListener);
    subscribeToSegmentMetadataChanges();
  }

  /**
   * subscribes to the changes in segment metadata in helix property store
   */
  private void subscribeToSegmentMetadataChanges() {

    helixPropertyStore.subscribe(metadataPathinZK, invoker);
  }

  /**
   * un-subscribes to the changes in segment metadata in helix property store
   */
  private void unsubscribeToSegmentMetadataChanges() {
    helixPropertyStore.unsubscribe(metadataPathinZK, invoker);
  }

  /**
   * Calculates the path in ZK for the metadata.
   * @return
   */
  private String calculateSegmentMetadataPath() {
    return "/" + StringUtil.join("/", resourceName, segmentName);
  }

  private KafkaSegmentMetadataChangeListener createMetadataChangeListener() {
    return new KafkaSegmentMetadataChangeListener(this);
  }

  private StreamProviderConfig getStreamProviderConfig(RealtimeIndexingConfig realtimeIndexingConfig) {
    
    return null;
  }

  private KafkaStreamProvider getStreamProvider(StreamProviderConfig streamProviderConfig) {
    return null;
  }

  private RealtimeSegment createRealtimeSegment(RealtimeIndexingConfig realtimeIndexingConfig) {
    return null;
  }

  @Override
  public void start() {
    long startOffset = 0; // get the start offset from segment metadata;
    kafkaStreamProvider.setOffset(startOffset);
    kafkaStreamProvider.start();
    while (true) {
      GenericRow next = kafkaStreamProvider.next();
      realtimeSegment.index(next);
      // continue until one of the following happens
      // reach the segment threshold, size,count,time interval
      // shutdown called explicitly
    }
  }

  @Override
  public MutableIndexSegment getRealtimeSegment() {
    return realtimeSegment;
  }

  @Override
  public void convertToOffline() {
    // convert the segment from inmemory format to on disk format and option #1.
    // swap the
    // segment in instanceDataManager or #option 2. we continue to call it
    // realtime segment
    // but loaded from disk ?
  }

  @Override
  public void shutdown() {
    kafkaStreamProvider.shutdown();
    unsubscribeToSegmentMetadataChanges();
  }

  private static class SegmentMetadataListenerInvoker implements HelixPropertyListener {

    private SegmentMetadataListener listener;

    public SegmentMetadataListenerInvoker(HelixManager manager, SegmentMetadataListener listener) {
      this.listener = listener;

    }

    @Override
    public void onDataChange(String path) {
      SegmentMetadataChangeContext changeContext = new SegmentMetadataChangeContext();
      SegmentMetadata segmentMetadata = null;
      listener.onChange(changeContext, segmentMetadata);
    }

    @Override
    public void onDataCreate(String path) {
      SegmentMetadataChangeContext changeContext = new SegmentMetadataChangeContext();
      SegmentMetadata segmentMetadata = null;
      listener.onChange(changeContext, segmentMetadata);

    }

    @Override
    public void onDataDelete(String path) {
      SegmentMetadataChangeContext changeContext = new SegmentMetadataChangeContext();
      SegmentMetadata segmentMetadata = null;
      listener.onChange(changeContext, segmentMetadata);

    }

  }

  public boolean hasStarted() {
    return false;
  }

  public boolean isClosed() {
    // TODO Auto-generated method stub
    return false;
  }

  public void pause() {
    // TODO Auto-generated method stub
    
  }

  public long getCurrentOffset() {
    // TODO Auto-generated method stub
    return 0;
  }

  public void setEndOffset(long endOffset) {
    // TODO Auto-generated method stub
    
  }

  public void waitUntilOffset(long endOffset) {
    // TODO Auto-generated method stub
    
  }

  public void reset() {
    // TODO Auto-generated method stub
    
  }
}
