package org.apache.pinot.common.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class YammerServerPrometheusMetricsTest extends ServerPrometheusMetricsTest {

  protected ServerMetrics _serverMetrics;

  @BeforeClass
  public void setup()
      throws Exception {
    _serverMetrics = new ServerMetrics(getPinotMetricsFactory().getPinotMetricsRegistry());
  }

  @Test(dataProvider = "serverTimers")
  public void timerTest(ServerTimer serverTimer) {
    if (serverTimer.isGlobal()) {
      _serverMetrics.addTimedValue(serverTimer, 30_000, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      _serverMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      _serverMetrics.addTimedTableValue(ExportedLabelValues.TABLENAME, serverTimer, 30_000L, TimeUnit.MILLISECONDS);

      assertTimerExportedCorrectly(serverTimer.getTimerName(), ExportedLabels.TABLENAME_TABLETYPE,
          EXPORTED_METRIC_PREFIX);
      assertTimerExportedCorrectly(serverTimer.getTimerName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "serverMeters")
  public void meterTest(ServerMeter serverMeter) {
    if (serverMeter.isGlobal()) {
      _serverMetrics.addMeteredGlobalValue(serverMeter, 4L);
      //we cannot use raw meter names for all meters as exported metrics don't follow any convention currently.
      // For example, meters that track realtime exceptions start with prefix "realtime_exceptions"
      if (meterTrackingRealtimeExceptions(serverMeter)) {
        assertMeterExportedCorrectly(getRealtimeExceptionMeterName(serverMeter));
      } else {
        assertMeterExportedCorrectly(serverMeter.getMeterName());
      }
    } else {
      if (METERS_ACCEPTING_CLIENT_ID.contains(serverMeter)) {
        addMeterWithLabels(serverMeter, CLIENT_ID);
        assertMeterExportedCorrectly(serverMeter.getMeterName(),
            ExportedLabels.PARTITION_TABLENAME_TABLETYPE_KAFKATOPIC);
      } else if (METERS_ACCEPTING_RAW_TABLE_NAMES.contains(serverMeter)) {
        addMeterWithLabels(serverMeter, ExportedLabelValues.TABLENAME);
        assertMeterExportedCorrectly(serverMeter.getMeterName(), ExportedLabels.TABLENAME);
      } else {
        //we pass tableNameWithType to all remaining meters
        addMeterWithLabels(serverMeter, TABLE_NAME_WITH_TYPE);
        assertMeterExportedCorrectly(serverMeter.getMeterName(), ExportedLabels.TABLENAME_TABLETYPE);
      }
    }
  }

  @Test(dataProvider = "serverGauges")
  public void gaugeTest(ServerGauge serverGauge) {
    if (serverGauge.isGlobal()) {
      _serverMetrics.setValueOfGlobalGauge(serverGauge, 10L);
      assertGaugeExportedCorrectly(serverGauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
    } else {
      if (serverGauge == ServerGauge.DEDUP_PRIMARY_KEYS_COUNT) {
        //this gauge is currently exported as: `pinot_server_${partitionId}_Value{database="dedupPrimaryKeysCount",
        // table="dedupPrimaryKeysCount.myTable",tableType="REALTIME",}`. We add an explicit test for it to maintain
        // backward compatibility. todo: ServerGauge.DEDUP_PRIMARY_KEYS_COUNT should be moved to
        //  gaugesThatAcceptPartition. It should be exported as:
        //  `pinot_server_dedupPrimaryKeysCount_Value{partition="3", table="myTable",tableType="REALTIME",}`
        addPartitionGaugeWithLabels(serverGauge, TABLE_NAME_WITH_TYPE);
        assertGaugeExportedCorrectly(String.valueOf(3),
            List.of(ExportedLabelKeys.DATABASE, serverGauge.getGaugeName(), ExportedLabelKeys.TABLE,
                "dedupPrimaryKeysCount.myTable", ExportedLabelKeys.TABLETYPE, ExportedLabelValues.TABLETYPE_REALTIME),
            EXPORTED_METRIC_PREFIX);
      } else if (GAUGES_ACCEPTING_CLIENT_ID.contains(serverGauge)) {
        addGaugeWithLabels(serverGauge, CLIENT_ID);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(),
            ExportedLabels.PARTITION_TABLENAME_TABLETYPE_KAFKATOPIC, EXPORTED_METRIC_PREFIX);
      } else if (GAUGES_ACCEPTING_PARTITION.contains(serverGauge)) {
        addPartitionGaugeWithLabels(serverGauge, TABLE_NAME_WITH_TYPE);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), ExportedLabels.PARTITION_TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      } else if (GAUGES_ACCEPTING_RAW_TABLE_NAME.contains(serverGauge)) {
        addGaugeWithLabels(serverGauge, ExportedLabelValues.TABLENAME);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
      } else {
        addGaugeWithLabels(serverGauge, TABLE_NAME_WITH_TYPE);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), ExportedLabels.TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      }
    }
  }

  private void addGaugeWithLabels(ServerGauge serverGauge, String labels) {
    _serverMetrics.setValueOfTableGauge(labels, serverGauge, 100L);
  }

  private void addPartitionGaugeWithLabels(ServerGauge serverGauge, String labels) {
    _serverMetrics.setValueOfPartitionGauge(labels, 3, serverGauge, 100L);
  }

  public void addMeterWithLabels(ServerMeter serverMeter, String labels) {
    _serverMetrics.addMeteredTableValue(labels, serverMeter, 4L);
  }

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }

  private boolean meterTrackingRealtimeExceptions(ServerMeter serverMeter) {
    return serverMeter == ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS
        || serverMeter == ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS
        || serverMeter == ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS || serverMeter == ServerMeter.UNCAUGHT_EXCEPTIONS;
  }

  private String getRealtimeExceptionMeterName(ServerMeter serverMeter) {
    String meterName = serverMeter.getMeterName();
    return "realtime_exceptions_" + meterName.substring(0, meterName.lastIndexOf("Exceptions"));
  }

  private void assertMeterExportedCorrectly(String exportedMeterName) {
    assertMeterExportedCorrectly(exportedMeterName, EXPORTED_METRIC_PREFIX);
  }

  private void assertMeterExportedCorrectly(String exportedMeterName, List<String> labels) {
    assertMeterExportedCorrectly(exportedMeterName, labels, EXPORTED_METRIC_PREFIX);
  }
}
