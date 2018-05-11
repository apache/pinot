package com.linkedin.thirdeye.detection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DataProviderTest {
  private DAOTestBase testBase;
  private EventManager eventDAO;
  private MergedAnomalyResultManager anomalyDAO;

  private DataProvider provider;

  private List<Long> eventIds;
  private List<Long> anomalyIds;

  @BeforeMethod
  public void beforeMethod() {
    this.testBase = DAOTestBase.getInstance();

    DAORegistry reg = DAORegistry.getInstance();
    this.eventDAO = reg.getEventDAO();
    this.anomalyDAO = reg.getMergedAnomalyResultDAO();

    this.eventIds = new ArrayList<>();
    this.eventIds.add(this.eventDAO.save(makeEvent(3600000L, 7200000L)));
    this.eventIds.add(this.eventDAO.save(makeEvent(10800000L, 14400000L)));
    this.eventIds.add(this.eventDAO.save(makeEvent(14400000L, 18000000L, Arrays.asList("a=1", "b=4", "b=2"))));
    this.eventIds.add(this.eventDAO.save(makeEvent(604800000L, 1209600000L, Arrays.asList("b=2", "c=3"))));
    this.eventIds.add(this.eventDAO.save(makeEvent(1209800000L, 1210600000L, Collections.singleton("b=4"))));

    this.anomalyIds = new ArrayList<>();
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 100L, 4000000L, 8000000L, Arrays.asList("a=1", "c=3", "b=2"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 100L, 8000000L, 12000000L, Arrays.asList("a=1", "c=4"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 200L, 604800000L, 1209600000L, Collections.<String>emptyList())));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 200L, 14400000L, 18000000L, Arrays.asList("a=1", "c=3"))));

    this.provider = new DefaultDataProvider(null, this.eventDAO, this.anomalyDAO, null, null, null);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    this.testBase.cleanup();
  }

  //
  // metric
  //

  // TODO fetch metric tests

  //
  // timeseries
  //

  // TODO fetch timeseries tests

  //
  // aggregates
  //

  // TODO fetch aggregates tests
  
  //
  // events
  //

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEventInvalid() {
    this.provider.fetchEvents(Collections.singleton(new EventSlice()));
  }

  @Test
  public void testEventSingle() {
    EventSlice slice = makeEventSlice(4000000L, 4100000L, Collections.<String>emptyList());

    Collection<EventDTO> events = this.provider.fetchEvents(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(events.size(), 1);
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(0), 3600000L, 7200000L, Collections.<String>emptyList())));
  }

  @Test
  public void testEventDimension() {
    EventSlice slice = makeEventSlice(10000000L, 1200000000L, Collections.singleton("b=2"));

    Collection<EventDTO> events = this.provider.fetchEvents(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(events.size(), 3);
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(1), 10800000L, 14400000L, Collections.<String>emptyList())));
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(2), 14400000L, 18000000L, Arrays.asList("a=1", "b=4", "b=2"))));
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(3), 604800000L, 1209600000L, Arrays.asList("b=2", "c=3"))));
  }

  //
  // anomalies
  //

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAnomalyInvalid() {
    this.provider.fetchAnomalies(Collections.singleton(new AnomalySlice()));
  }

  @Test
  public void testAnomalySingle() {
    AnomalySlice slice = makeAnomalySlice(-1, 1209000000L, -1, Collections.<String>emptyList());

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(2), 200L, 604800000L, 1209600000L, Collections.<String>emptyList())));
  }

  @Test
  public void testAnomalyDimension() {
    AnomalySlice slice = makeAnomalySlice(-1, 0, -1, Arrays.asList("a=1", "c=3"));

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(0), 100L, 4000000L, 8000000L, Arrays.asList("a=1", "c=3", "b=2"))));
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(2), 200L, 604800000L, 1209600000L, Collections.<String>emptyList())));
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(3), 200L, 14400000L, 18000000L, Arrays.asList("a=1", "c=3"))));
  }

  //
  // utils
  //

  private static MergedAnomalyResultDTO makeAnomaly(Long id, Long configId, long start, long end, Iterable<String> filterStrings) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setDetectionConfigId(configId);
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);
    anomaly.setId(id);

    DimensionMap filters = new DimensionMap();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      filters.put(parts[0], parts[1]);
    }

    anomaly.setDimensions(filters);
    return anomaly;
  }

  private static EventDTO makeEvent(long start, long end) {
    return makeEvent(null, start, end, Collections.<String>emptyList());
  }

  private static EventDTO makeEvent(long start, long end, Iterable<String> filterStrings) {
    return makeEvent(null, start, end, filterStrings);
  }

  private static EventDTO makeEvent(Long id, long start, long end, Iterable<String> filterStrings) {
    EventDTO event = new EventDTO();
    event.setId(id);
    event.setName(String.format("event-%d-%d", start, end));
    event.setStartTime(start);
    event.setEndTime(end);

    Map<String, List<String>> filters = new HashMap<>();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      if (!filters.containsKey(parts[0])) {
        filters.put(parts[0], new ArrayList<String>());
      }
      filters.get(parts[0]).add(parts[1]);
    }

    event.setTargetDimensionMap(filters);

    return event;
  }

  private static EventSlice makeEventSlice(long start, long end, Iterable<String> filterStrings) {
    SetMultimap<String, String> filters = HashMultimap.create();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      filters.put(parts[0], parts[1]);
    }
    return new EventSlice(start, end, filters);
  }

  private static AnomalySlice makeAnomalySlice(long configId, long start, long end, Iterable<String> filterStrings) {
    SetMultimap<String, String> filters = HashMultimap.create();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      filters.put(parts[0], parts[1]);
    }
    return new AnomalySlice(configId, start, end, filters);
  }
}
