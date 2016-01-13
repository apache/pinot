package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoReduceOutput;
import com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeJob.RollupPhaseThreeReducer;

/**
 * @author ajaspal
 */

public class TestRollUpPhase3 {
  private static final String CONF_FILE = "config.yml";
  private static Integer ROLLUP_THRESHOLD;
  private static ReduceDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> reduceDriver;
  private static RollupPhaseThreeConfig rollUpConfig;

  @BeforeSuite
  public void setUp() throws Exception {
    RollupPhaseThreeReducer reducer = new RollupPhaseThreeReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    Configuration configuration = reduceDriver.getConfiguration();
    configuration.set(RollupPhaseThreeConstants.ROLLUP_PHASE3_CONFIG_PATH.toString(),
        ClassLoader.getSystemResource(CONF_FILE).toString());
    Path configPath = new Path(ClassLoader.getSystemResource(CONF_FILE).toString());
    FileSystem fileSystem = FileSystem.get(configuration);
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
    rollUpConfig = RollupPhaseThreeConfig.fromStarTreeConfig(starTreeConfig);
    ROLLUP_THRESHOLD = Integer.parseInt(rollUpConfig.getThresholdFuncParams().get("threshold"));
  }

  private List<Pair<BytesWritable, List<BytesWritable>>> generateTestReducerData()
      throws Exception {
    List<Pair<BytesWritable, List<BytesWritable>>> inputRecords =
        new ArrayList<Pair<BytesWritable, List<BytesWritable>>>();
    List<BytesWritable> list = new ArrayList<BytesWritable>();
    String[] input1 = {
        "A1", "B1", "C1"
    };
    DimensionKey rawKey = new DimensionKey(input1);
    MetricTimeSeries rawSeries =
        TestHelper.generateMetricTimeSeries(rollUpConfig, ROLLUP_THRESHOLD - 10);

    String[] combination1 = {
        "A1", "B1", "C1"
    };
    MetricTimeSeries timeSeries = rawSeries;
    DimensionKey combinationKey = new DimensionKey(combination1);
    RollupPhaseTwoReduceOutput output =
        new RollupPhaseTwoReduceOutput(combinationKey, timeSeries, rawKey, rawSeries);
    list.add(new BytesWritable(output.toBytes()));

    String[] combination2 = {
        "?", "B1", "C1"
    };
    timeSeries = TestHelper.generateMetricTimeSeries(rollUpConfig, ROLLUP_THRESHOLD - 5);
    combinationKey = new DimensionKey(combination2);
    output = new RollupPhaseTwoReduceOutput(combinationKey, timeSeries, rawKey, rawSeries);
    list.add(new BytesWritable(output.toBytes()));

    String[] combination3 = {
        "?", "?", "C1"
    };
    timeSeries = TestHelper.generateMetricTimeSeries(rollUpConfig, ROLLUP_THRESHOLD + 1);
    combinationKey = new DimensionKey(combination3);
    output = new RollupPhaseTwoReduceOutput(combinationKey, timeSeries, rawKey, rawSeries);
    list.add(new BytesWritable(output.toBytes()));

    String[] combination4 = {
        "?", "?", "?"
    };
    timeSeries = TestHelper.generateMetricTimeSeries(rollUpConfig, ROLLUP_THRESHOLD + 10);
    combinationKey = new DimensionKey(combination4);
    output = new RollupPhaseTwoReduceOutput(combinationKey, timeSeries, rawKey, rawSeries);
    list.add(new BytesWritable(output.toBytes()));

    BytesWritable keyWritable = new BytesWritable();
    keyWritable.set(rawKey.toMD5(), 0, rawKey.toMD5().length);
    Pair<BytesWritable, List<BytesWritable>> record =
        new Pair<BytesWritable, List<BytesWritable>>(keyWritable, list);
    inputRecords.add(record);

    return inputRecords;
  }

  @Test
  public void testRollUpPhase3() throws Exception {
    List<Pair<BytesWritable, List<BytesWritable>>> inputRecordsReducer = generateTestReducerData();
    reduceDriver.addAll(inputRecordsReducer);
    List<Pair<BytesWritable, BytesWritable>> result = reduceDriver.run();
    Assert.assertEquals(1, result.size());
    Pair<BytesWritable, BytesWritable> p = result.get(0);
    DimensionKey selectedRollUp = DimensionKey.fromBytes(p.getFirst().getBytes());
    String[] combination3 = {
        "?", "?", "C1"
    };
    DimensionKey expectedKey = new DimensionKey(combination3);

    // make sure that the correct dimension combination was selected for rollup
    Assert.assertEquals(selectedRollUp, expectedKey);

    // make sure that the correct value is emmitted by the reducer
    // TODO: getting EOF error in MetricTimeSeries class
    // Assert.assertEquals(MetricTimeSeries.fromBytes(p.getSecond().getBytes(),
    // TestHelper.getMetricSchema(rollUpConfig)),
    // MetricTimeSeries.fromBytes(inputRecordsReducer.get(0).getSecond().get(2).getBytes(),
    // TestHelper.getMetricSchema(rollUpConfig)));
  }
}

class TestHelper {
  public static MetricTimeSeries generateMetricTimeSeries(RollupPhaseThreeConfig rollUpConfig,
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

  public static MetricSchema getMetricSchema(RollupPhaseThreeConfig rollUpConfig) {
    List<String> names = rollUpConfig.getMetricNames();
    List<MetricType> types = rollUpConfig.getMetricTypes();
    return new MetricSchema(names, types);
  }
}
