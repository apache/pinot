package com.linkedin.thirdeye.detector.email;

import java.awt.BasicStroke;
import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickUnit;
import org.jfree.chart.axis.DateTickUnitType;
import org.jfree.chart.plot.IntervalMarker;
import org.jfree.chart.plot.Marker;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.RegularTimePeriod;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.comparison.Row;
import com.linkedin.thirdeye.datasource.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.datasource.comparison.Row.Metric;

/** Creates JFreeChart images from Thirdeye Data. */
public class AnomalyGraphGenerator {
  private static final AnomalyGraphGenerator INSTANCE = new AnomalyGraphGenerator();

  private static final int NUM_X_TICKS = 12;
  private static final Color TRANSPARENT_GRAY = new Color(128, 128, 128, 50); // default grays are
                                                                              // too opaque
  private static final String CURRENT_LABEL = "Current";
  private static final String BASELINE_LABEL = "Last Week"; // TODO make w/w configurable.
  private static final int DEFAULT_CHART_HEIGHT = 480;
  private static final int DEFAULT_CHART_WIDTH = 720;
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyGraphGenerator.class);

  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");

  // Credit for initial skeleton code:
  // http://www.java2s.com/Code/Java/Chart/JFreeChartLineChartDemo6.htm
  public static AnomalyGraphGenerator getInstance() {
    return INSTANCE;
  }

  /**
   * Creates a new graph generator.
   * @throws Exception
   */
  private AnomalyGraphGenerator() {
  }

  /**
   * Generates a JFreeChart object corresponding to the provided response data. See
   * {@link #createChart(TimeOnTimeComparisonResponse, TimeGranularity, long, Map).
   */
  public JFreeChart createChart(final TimeOnTimeComparisonResponse data,
      final TimeGranularity timeGranularity, final long windowMillis,
      final Map<RawAnomalyResultDTO, String> anomaliesWithLabels) {
    Set<String> metrics = data.getMetrics();
    // TODO error if more than one metric?
    String metric = metrics.iterator().next();
    LOG.info("Creating time series collections for {}", metric);
    final TimeSeriesCollection dataset = createTimeSeries(data);
    return createChart(dataset, metric, timeGranularity, windowMillis, anomaliesWithLabels);
  }

  /**
   * Creates a chart containing the current/baseline data (in that order) as well as markers for
   * each anomaly interval. timeGranularity and windowMillis are used to determine the date format
   * and spacing for tick marks on the domain (x) axis.
   */
  public JFreeChart createChart(final XYDataset dataset, final String metric,
      final TimeGranularity timeGranularity, final long windowMillis,
      final Map<RawAnomalyResultDTO, String> anomaliesWithLabels) {

    // create the chart...
    final JFreeChart chart = ChartFactory.createTimeSeriesChart(null, // no chart title for email
                                                                      // image
        "Date (" + DEFAULT_TIME_ZONE.getID() + ")", // x axis label
        metric, // y axis label
        dataset, // data
        true, // include legend
        false, // tooltips - n/a if the chart will be saved as an img
        false // urls - n/a if the chart will be saved as an img
    );

    // get a reference to the plot for further customisation...
    final XYPlot plot = chart.getXYPlot();
    plot.setBackgroundPaint(Color.white);
    plot.setDomainGridlinesVisible(false);
    plot.setRangeGridlinesVisible(false);

    // dashboard webapp currently uses solid blue for current and dashed blue for baseline
    // (5/2/2016)
    final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
    renderer.setSeriesShapesVisible(0, false);
    renderer.setSeriesShapesVisible(1, false);
    renderer.setSeriesPaint(0, Color.BLUE);
    renderer.setSeriesPaint(1, Color.BLUE);
    // http://www.java2s.com/Code/Java/Chart/JFreeChartLineChartDemo5showingtheuseofacustomdrawingsupplier.htm
    // set baseline to be dashed
    renderer.setSeriesStroke(1,
        new BasicStroke(2.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f, new float[] {
            2.0f, 6.0f
        }, 0.0f));
    plot.setRenderer(renderer);

    DateAxis axis = (DateAxis) plot.getDomainAxis();
    DateTickUnit dateTickUnit = getDateTickUnit(timeGranularity, windowMillis);
    SimpleDateFormat dateFormat = getDateFormat(timeGranularity);
    axis.setDateFormatOverride(dateFormat);
    axis.setTickUnit(dateTickUnit);
    axis.setVerticalTickLabels(true);

    List<Marker> anomalyIntervals = getAnomalyIntervals(anomaliesWithLabels);
    for (Marker marker : anomalyIntervals) {
      plot.addDomainMarker(marker);
    }

    return chart;

  }

  /**
   * Returns data with series 0 = current and 1 = baseline. This method assumes there is exactly one
   * metric in the response data.
   */
  private TimeSeriesCollection createTimeSeries(final TimeOnTimeComparisonResponse data) {
    final TimeSeries baseline = new TimeSeries(BASELINE_LABEL);
    final TimeSeries current = new TimeSeries(CURRENT_LABEL);
    for (int i = 0; i < data.getNumRows(); i++) {
      Row row = data.getRow(i);
      // Plot the baseline data as an overlay on the corresponding current data point.
      // long baselineStart = row.getBaselineStart().getMillis();
      long currentStart = row.getCurrentStart().getMillis();
      Date date = new Date(currentStart);
      RegularTimePeriod timePeriod = new Millisecond(date);
      Metric metric = row.getMetrics().get(0);
      baseline.add(timePeriod, metric.getBaselineValue());
      current.add(timePeriod, metric.getCurrentValue());
    }

    final TimeSeriesCollection dataset = new TimeSeriesCollection();
    dataset.addSeries(current);
    dataset.addSeries(baseline);
    return dataset;
  }

  /**
   * Merges overlapping anomalies and creates JFreeChart Markers for each merged point or interval.
   */
  private List<Marker> getAnomalyIntervals(Map<RawAnomalyResultDTO, String> anomaliesWithLabels) {
    TreeMap<RawAnomalyResultDTO, String> chronologicalAnomaliesWithLabels =
        new TreeMap<RawAnomalyResultDTO, String>(new Comparator<RawAnomalyResultDTO>() {
          @Override
          public int compare(RawAnomalyResultDTO o1, RawAnomalyResultDTO o2) {
            int diff = Long.compare(o1.getStartTime(), o2.getStartTime());
            if (diff == 0) {
              diff = o1.compareTo(o2);
            }
            return diff;
          }
        });
    chronologicalAnomaliesWithLabels.putAll(anomaliesWithLabels);

    Long intervalStart = null;
    Long intervalEnd = null;
    // StringBuilder labelBuilder = new StringBuilder();
    List<Marker> anomalyMarkers = new ArrayList<>();
    for (Entry<RawAnomalyResultDTO, String> entry : chronologicalAnomaliesWithLabels.entrySet()) {
      RawAnomalyResultDTO anomalyResult = entry.getKey();
      // String label = entry.getValue();
      Long anomalyStart = anomalyResult.getStartTime();
      Long anomalyEnd = anomalyResult.getEndTime();
      anomalyEnd = anomalyEnd == null ? anomalyStart : anomalyEnd;
      if (intervalStart == null || anomalyStart > intervalEnd) {
        // initialization of intervals
        if (intervalStart != null) {
          // create a new marker if this isn't the first element/initialization
          Marker anomalyMarker = createGraphMarker(intervalStart, intervalEnd, null);// ,
                                                                                     // labelBuilder.toString());
          // labelBuilder.setLength(0);
          anomalyMarkers.add(anomalyMarker);
        }
        intervalStart = anomalyStart;
        intervalEnd = anomalyEnd;

      } else {
        intervalEnd = Math.max(intervalEnd, anomalyEnd);
      }
      // if (labelBuilder.length() > 0) {
      // labelBuilder.append(",");
      // }
      // labelBuilder.append(label);
    }
    // add the last marker
    if (intervalStart != null) {
      Marker anomalyMarker = createGraphMarker(intervalStart, intervalEnd, null);// labelBuilder.toString());
      anomalyMarkers.add(anomalyMarker);
    }

    // Optional: determine marker positions relative to each other (staggered Left -> Right by
    // descending
    // height), aligned so that the marker is on the right
    // int labelCounter = 0;
    // for (Marker anomalyMarker : anomalyMarkers) {
    // anomalyMarker.setLabelAnchor(RectangleAnchor.TOP_LEFT);
    // anomalyMarker.setLabelTextAnchor(TextAnchor.TOP_RIGHT);
    // anomalyMarker.setLabelOffset(new RectangleInsets((labelCounter++
    // * (DEFAULT_CHART_HEIGHT / (anomalyMarkers.size() + 1)) % DEFAULT_CHART_HEIGHT), 0, 0, 0));
    // }
    return anomalyMarkers;
  }

  /**
   * Returns either a value marker (point) or a interval marker (range) depending on provided
   * inputs. By default values are gray or transparent gray.
   * @param intervalStart
   * @param intervalEnd
   * @return
   */
  private Marker createGraphMarker(Long intervalStart, Long intervalEnd, String label) {
    Marker anomalyMarker;
    if (intervalEnd == null || intervalStart.equals(intervalEnd)) {
      // Point
      anomalyMarker = new ValueMarker(intervalStart);
      anomalyMarker.setPaint(Color.LIGHT_GRAY);
    } else {
      // Range
      anomalyMarker = new IntervalMarker(intervalStart, intervalEnd);
      anomalyMarker.setPaint(TRANSPARENT_GRAY);
    }

    anomalyMarker.setLabel(label);
    LOG.info("Anomaly marker generated for: {}, {}", intervalStart, intervalEnd);
    return anomalyMarker;
  }

  private SimpleDateFormat getDateFormat(final TimeGranularity timeGranularity) {
    String format;
    switch (timeGranularity.getUnit()) {
    case DAYS:
      format = "MM/dd";
      break;
    case HOURS:
      format = "MM/dd-HH'H'";
      break;
    case MILLISECONDS:
      format = "HH:mm:ss:SSS";
      break;
    case MINUTES:
      format = "HH:mm";
      break;
    case SECONDS:
      format = "HH:mm:ss";
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported time unit granularity: " + timeGranularity.getUnit());
    }
    SimpleDateFormat dateFormat = new SimpleDateFormat(format);
    dateFormat.setTimeZone(DEFAULT_TIME_ZONE);
    return dateFormat;
  }

  private DateTickUnit getDateTickUnit(final TimeGranularity timeGranularity,
      final long windowMillis) {
    long windowBuckets = timeGranularity.convertToUnit(windowMillis);
    int tickSize = (int) Math.ceil(windowBuckets / (double) NUM_X_TICKS);
    DateTickUnitType unitType;
    switch (timeGranularity.getUnit()) {
    case DAYS:
      unitType = DateTickUnitType.DAY;
      break;
    case HOURS:
      unitType = DateTickUnitType.HOUR;
      break;
    case MILLISECONDS:
      unitType = DateTickUnitType.MILLISECOND;
      break;
    case MINUTES:
      unitType = DateTickUnitType.MINUTE;
      break;
    case SECONDS:
      unitType = DateTickUnitType.SECOND;
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported time unit granularity: " + timeGranularity.getUnit());
    }
    return new DateTickUnit(unitType, tickSize);
  }

  public void writeChartToFile(JFreeChart chart, String filepath) throws IOException {
    File file = new File(filepath);
    LOG.info("Writing to file: {}", file.getAbsolutePath());
    ChartUtilities.saveChartAsPNG(file, chart, DEFAULT_CHART_WIDTH, DEFAULT_CHART_HEIGHT);
  }

}
