package com.linkedin.thirdeye.bootstrap.rollup.phase4;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.bootstrap.rollup.phase4.RollupPhaseFourJob.RollupPhaseFourReducer;

/**
 * @author ajaspal
 */

public class TestRollUpPhase4 {
  private static final String CONF_FILE = "config.yml";
  private static ReduceDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> reduceDriver;
  private static RollupPhaseFourConfig rollUpConfig;

  @BeforeSuite
  public void setUp() throws Exception {
    RollupPhaseFourReducer reducer = new RollupPhaseFourReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    Configuration configuration = reduceDriver.getConfiguration();
    configuration.set(RollupPhaseFourConstants.ROLLUP_PHASE4_CONFIG_PATH.toString(),
        ClassLoader.getSystemResource(CONF_FILE).toString());
    Path configPath = new Path(ClassLoader.getSystemResource(CONF_FILE).toString());
    FileSystem fileSystem = FileSystem.get(configuration);
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
    rollUpConfig = RollupPhaseFourConfig.fromStarTreeConfig(starTreeConfig);
  }

  private List<Pair<BytesWritable, List<BytesWritable>>> generateTestReducerData()
      throws Exception {
    List<Pair<BytesWritable, List<BytesWritable>>> inputRecords =
        new ArrayList<Pair<BytesWritable, List<BytesWritable>>>();
    List<BytesWritable> list = new ArrayList<BytesWritable>();

    String[] combination = {
        "?", "?", "C1"
    };
    DimensionKey selectedRollUpKey = new DimensionKey(combination);
    MetricTimeSeries timeSeries = TestHelper.generateMetricTimeSeries(rollUpConfig, 10);
    list.add(new BytesWritable(timeSeries.toBytes()));

    timeSeries = TestHelper.generateMetricTimeSeries(rollUpConfig, 20);
    list.add(new BytesWritable(timeSeries.toBytes()));

    inputRecords.add(new Pair<BytesWritable, List<BytesWritable>>(
        new BytesWritable(selectedRollUpKey.toBytes()), list));
    return inputRecords;
  }

  @Test
  public void testRollUpPhaseFour() throws Exception {
    List<Pair<BytesWritable, List<BytesWritable>>> inputRecordsReducer = generateTestReducerData();
    reduceDriver.addAll(inputRecordsReducer);
    List<Pair<BytesWritable, BytesWritable>> result = reduceDriver.run();
    Assert.assertEquals(1, result.size());
    String[] combination = {
        "?", "?", "C1"
    };
    DimensionKey expectedKey = new DimensionKey(combination);
    DimensionKey outputKey = DimensionKey.fromBytes(result.get(0).getFirst().getBytes());
    Assert.assertEquals(outputKey, expectedKey);
    MetricTimeSeries outputValue = MetricTimeSeries.fromBytes(result.get(0).getSecond().getBytes(),
        TestHelper.getMetricSchema(rollUpConfig));
    MetricTimeSeries expectedValue = TestHelper.generateMetricTimeSeries(rollUpConfig, 30);
    Assert.assertEquals(expectedValue, outputValue);
  }
}

class TestHelper {
  public static MetricTimeSeries generateMetricTimeSeries(RollupPhaseFourConfig rollUpConfig,
      int value) {
    List<String> names = rollUpConfig.getMetricNames();
    List<MetricType> types = rollUpConfig.getMetricTypes();
    MetricSchema schema = new MetricSchema(names, types);
    MetricTimeSeries series = new MetricTimeSeries(schema);
    long timeStamp = TimeUnit.HOURS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    for (int i = 0; i < names.size(); i++) {
      series.set(timeStamp, names.get(i), value);
    }
    return series;
  }

  public static MetricSchema getMetricSchema(RollupPhaseFourConfig rollUpConfig) {
    List<String> names = rollUpConfig.getMetricNames();
    List<MetricType> types = rollUpConfig.getMetricTypes();
    return new MetricSchema(names, types);
  }
}
