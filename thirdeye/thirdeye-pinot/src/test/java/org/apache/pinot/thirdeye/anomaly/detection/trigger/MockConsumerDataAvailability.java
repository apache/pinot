package org.apache.pinot.thirdeye.anomaly.detection.trigger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class MockConsumerDataAvailability extends DataAvailabilityKafkaConsumer {
  private int callCount;

  public MockConsumerDataAvailability() {
    super("", "", "", new Properties());
    callCount = 0;
  }

  @Override
  public List<DataAvailabilityEvent> poll(long poll) {
    List<DataAvailabilityEvent> res = new ArrayList<>();
    if (callCount == 0) {
      res.add(createEvent(1, 1000, 2000));
      res.add(createEvent(2, 2000, 3000));
    } else if (callCount == 1) {
      res.add(createEvent(1, 2000, 3000));
      res.add(createEvent(1, 2000, 3000));
      res.add(createEvent(1, 1000, 2000));
      res.add(createEvent(2, 1000, 3000));
    } else if (callCount == 2) {
      res.add(createEvent(1, 0, 1000));
      res.add(createEvent(3, 1000, 2000));
      res.add(createEvent(4, 1000, 2000));
    }
    return res;
  }

  @Override
  public void commitSync() {
    callCount += 1;
  }

  @Override
  public void close() {

  }

  private static DataAvailabilityEvent createEvent(int suffix, long lowWatermark, long highWatermark) {
    String datasetPrefix = DataAvailabilityEventListenerTest.TEST_DATASET_PREFIX;
    String dataSource  = DataAvailabilityEventListenerTest.TEST_DATA_SOURCE;
    MockDataAvailabilityEvent event = new MockDataAvailabilityEvent();
    event.setDatasetName(datasetPrefix + suffix);
    event.setDataStore(dataSource);
    event.setLowWatermark(lowWatermark);
    event.setHighWatermark(highWatermark);
    return event;
  }
}
