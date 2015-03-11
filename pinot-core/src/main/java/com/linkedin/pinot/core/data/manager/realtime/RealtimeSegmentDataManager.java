package com.linkedin.pinot.core.data.manager.realtime;

import java.io.File;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelConsumerStreamProvider;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.core.segment.index.loader.Loaders;


public class RealtimeSegmentDataManager implements SegmentDataManager {
  private static final Logger logger = Logger.getLogger(RealtimeSegmentDataManager.class);

  private final Schema schema;
  private final ReadMode mode;
  private final RealtimeDataResourceZKMetadata resourceMetadata;
  private final InstanceZKMetadata instanceMetadata;
  private final RealtimeSegmentZKMetadata segmentMetatdaZk;

  private final StreamProviderConfig kafkaStreamProviderConfig;
  private final StreamProvider kafkaStreamProvider;
  private final File resourceDir;
  private final Object lock = new Object();
  private IndexSegment realtimeSegment;

  private final long start = System.currentTimeMillis();
  private boolean keepIndexing = true;
  private TimerTask segmentStatusTask;
  private final RealtimeResourceDataManager notifier;
  private Thread indexingThread;

  public RealtimeSegmentDataManager(final RealtimeSegmentZKMetadata segmentMetadata,
      final RealtimeDataResourceZKMetadata resourceMetadata, InstanceZKMetadata instanceMetadata,
      RealtimeResourceDataManager realtimeResourceManager, final String resourceDataDir, final ReadMode mode)
      throws Exception {
    this.schema = resourceMetadata.getDataSchema();
    this.resourceMetadata = resourceMetadata;
    this.instanceMetadata = instanceMetadata;
    this.segmentMetatdaZk = segmentMetadata;

    // create and init stream provider config
    // TODO : ideally resourceMetatda should create and give back a streamProviderConfig
    this.kafkaStreamProviderConfig = new KafkaHighLevelStreamProviderConfig();
    this.kafkaStreamProviderConfig.init(resourceMetadata, instanceMetadata);
    this.resourceDir = new File(resourceDataDir);
    this.mode = mode;
    // create and init stream provider
    this.kafkaStreamProvider = new KafkaHighLevelConsumerStreamProvider();
    this.kafkaStreamProvider.init(kafkaStreamProviderConfig);

    // lets create a new realtime segment
    realtimeSegment = new RealtimeSegmentImpl(schema);
    ((RealtimeSegmentImpl) (realtimeSegment)).setSegmentName(segmentMetadata.getSegmentName());
    notifier = realtimeResourceManager;

    segmentStatusTask = new TimerTask() {

      @Override
      public void run() {
        computeKeepIndexing();
      }
    };

    // start a schedule timer to keep track of the segment
    TimerService.timer.schedule(segmentStatusTask, 1000 * 60, 1000 * 60 * 30);

    // start the indexing thread
    indexingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        // continue indexing until critertia is met
        while (index()) {

        }

        // kill the timer first
        segmentStatusTask.cancel();

        String tempFolder = "tmp-" + String.valueOf(System.currentTimeMillis());

        // lets convert the segment now
        RealtimeSegmentConverter conveter =
            new RealtimeSegmentConverter((RealtimeSegmentImpl) realtimeSegment, "/tmp/" + tempFolder, schema,
                resourceMetadata.getResourceName(), resourceMetadata.getTableList().get(0),
                segmentMetadata.getSegmentName());
        try {
          conveter.build();
          FileUtils.moveDirectory(new File("/tmp/" + tempFolder).listFiles()[0], new File(resourceDataDir,
              segmentMetadata.getSegmentName()));
          swap();
          RealtimeSegmentZKMetadata metadaToOverrite = new RealtimeSegmentZKMetadata();
          metadaToOverrite.setResourceName(resourceMetadata.getResourceName());
          metadaToOverrite.setTableName(resourceMetadata.getTableList().get(0));
          metadaToOverrite.setSegmentName(segmentMetadata.getSegmentName());
          metadaToOverrite.setSegmentType(SegmentType.OFFLINE);
          metadaToOverrite.setStatus(Status.DONE);
          metadaToOverrite.setStartTime(((RealtimeSegmentImpl) realtimeSegment).getMinTime());
          metadaToOverrite.setEndTime(((RealtimeSegmentImpl) realtimeSegment).getMaxTime());

          notifier.notify(metadaToOverrite);

          kafkaStreamProvider.commit();
          kafkaStreamProvider.shutdown();
        } catch (Exception e) {
          logger.error(e);
        }
      }
    });

    indexingThread.start();
  }

  public void swap() throws Exception {
    IndexSegment segment = Loaders.IndexSegment.load(new File(resourceDir, segmentMetatdaZk.getSegmentName()), mode);
    synchronized (lock) {
      this.realtimeSegment = segment;
    }
  }

  @Override
  public IndexSegment getSegment() {
    // TODO Auto-generated method stub
    return realtimeSegment;
  }

  @Override
  public String getSegmentName() {
    // TODO Auto-generated method stub
    return null;
  }

  private void computeKeepIndexing() {
    if (!keepIndexing) {
      return;
    }

    if (System.currentTimeMillis() - start >= (60 * 60 * 1000)) {
      keepIndexing = false;
    }

    if (((RealtimeSegmentImpl) realtimeSegment).getRawDocumentCount() >= 10000000) {
      keepIndexing = false;
    }
  }

  public boolean index() {
    if (keepIndexing) {
      ((RealtimeSegmentImpl) realtimeSegment).index(kafkaStreamProvider.next());
      return true;
    }

    return false;
  }
}
