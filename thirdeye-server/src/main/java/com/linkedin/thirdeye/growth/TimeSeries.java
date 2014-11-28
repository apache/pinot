package com.linkedin.thirdeye.growth;

import java.awt.BasicStroke;
import java.awt.Color;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.swing.JPanel;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class TimeSeries extends ApplicationFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static String SERVER= "gbrandt-ld:10000";
	public TimeSeries(String title) throws Exception {
		super(title);
		int start = 392815;
		int end = start + 481;
		int topK = 10;
		Map<String, Set<String>> dimensionsAll = getDimensionValues();
		Map<String, Set<String>> dimensionsTopK = getTopK(dimensionsAll, start, start +24, topK);
		JPanel panel = new JPanel();
		for (String dimension : dimensionsTopK.keySet()) {
			final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
			Set<String> set = dimensionsTopK.get(dimension);
			for (String dimensionValue : set) {
				title = dimension + "=" + dimensionValue;
				int current = start;
				int step = 24;
				while (current < end) {
					String urlString = String.format(
							"http://" + SERVER +"/metrics/abook/%s/%s?",
							current, current + step)
							+ title;
					System.out.println(urlString);
					URL url = new URL(urlString);
					URLConnection yc = url.openConnection();
					BufferedReader in = null;
					try {
						in = new BufferedReader(new InputStreamReader(
								yc.getInputStream(), Charset.forName("UTF-8")));
						String inputLine;
						while ((inputLine = in.readLine()) != null) {
							// System.out.println(inputLine);
							JsonNode rootNode = new ObjectMapper()
									.readTree(inputLine.getBytes());
							List<String> numImportedContacts = rootNode
									.findValuesAsText("numberOfMemberConnectionsSent");
							Timestamp timestamp = new Timestamp(
									(long) current * 60 * 60 * 1000);
							System.out.println(timestamp + "="
									+ numImportedContacts);
							long numImportedContactsMetric = Long
									.parseLong(numImportedContacts.get(0));
							dataset.addValue(numImportedContactsMetric,
									dimensionValue, "" + (current - start));
						}
					} catch (Exception e) {

					} finally {
						if (in != null) {
							in.close();
						}
					}
					current = current + step;
				}
			}
			final JFreeChart chart = createChart(dataset, dimension);
			final ChartPanel chartPanel = new ChartPanel(chart);
			chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
			panel.add(chartPanel);
		}
		setContentPane(panel);
	}

	static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(
			Map<K, V> map) {
		SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<Map.Entry<K, V>>(
				new Comparator<Map.Entry<K, V>>() {
					@Override
					public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
						int res = e2.getValue().compareTo(e1.getValue());
						return res != 0 ? res : 1;
					}
				});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

	public static Map<String, Set<String>> getTopK(
			Map<String, Set<String>> dimensions, int startTime, int endTime,
			int topK) throws Exception {
		Map<String, Map<String, Integer>> dimensionMetrics = new HashMap<String, Map<String, Integer>>();

		for (String dimensionName : dimensions.keySet()) {
			Set<String> dimensionValues = dimensions.get(dimensionName);
			Map<String, Integer> valueMetricMap = new HashMap<String, Integer>();
			dimensionMetrics.put(dimensionName, valueMetricMap);

			for (String dimensionValue : dimensionValues) {
				BufferedReader in = null;
				try {
					String urlString = String.format(
							"http://"+SERVER+"/metrics/abook/%s/%s?%s=%s",
							startTime, endTime, dimensionName, dimensionValue);
					System.out.println(urlString);
					URL url = new URL(urlString);
					URLConnection yc = url.openConnection();
					in = new BufferedReader(new InputStreamReader(
							yc.getInputStream()));
					String inputLine;
					while ((inputLine = in.readLine()) != null) {
						JsonNode rootNode = new ObjectMapper()
								.readTree(inputLine.getBytes());
						List<String> numImportedContacts = rootNode
								.findValuesAsText("numberOfMemberConnectionsSent");
						valueMetricMap.put(dimensionValue,
								Integer.parseInt(numImportedContacts.get(0)));

					}
				} catch (Exception e) {
					System.err.println(e.getMessage());
				} finally {
					if (in != null) {
						in.close();
					}
				}
			}
		}
		Map<String, Set<String>> map = new TreeMap<String, Set<String>>();

		for (String dimensionName : dimensions.keySet()) {
			Set<String> valueSet = new LinkedHashSet<String>();
			map.put(dimensionName, valueSet);
			SortedSet<Entry<String, Integer>> sortedByValues = entriesSortedByValues(dimensionMetrics
					.get(dimensionName));
			int count = 0;
			for (Entry<String, Integer> entry : sortedByValues) {
				valueSet.add(entry.getKey());
				count = count + 1;
				if (count == topK) {
					break;
				}
			}
		}
		return map;
	}

	public static Map<String, Set<String>> getDimensionValues()
			throws Exception {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		String urlString = String
				.format("http://"+ SERVER +"/dimensions/abook");
		System.out.println(urlString);
		URL url = new URL(urlString);
		URLConnection yc = url.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(
				yc.getInputStream()));
		String inputLine;
		while ((inputLine = in.readLine()) != null) {
			JsonNode rootNode = new ObjectMapper().readTree(inputLine
					.getBytes());
			Iterator<String> fieldNames = rootNode.getFieldNames();
			while (fieldNames.hasNext()) {
				String fieldName = (String) fieldNames.next();
				HashSet<String> valueSet = new HashSet<String>();
				map.put(fieldName, valueSet);
				System.out.println(fieldName);
				ArrayNode node = (ArrayNode) rootNode.path(fieldName);
				for (int i = 0; i < node.size(); i++) {
					String val = node.get(i).asText().replace("\"", "").trim();
					System.out.println(val);
					valueSet.add(val);
				}
				// System.out.println(node.isArray() + " " + node.getClass());

			}
			System.out.println();
		}
		in.close();
		return map;

	}

	/**
	 * Creates a chart.
	 * 
	 * @param dataset
	 *            the data for the chart.
	 * 
	 * @return a chart.
	 */
	private JFreeChart createChart(final CategoryDataset dataset, String chartTile) {

		// create the chart...
		final JFreeChart chart = ChartFactory.createLineChart(
				chartTile, // chart title
				"Time", // domain axis label
				"numberOfMemberConnectionsSent", // range axis label
				dataset, // data
				PlotOrientation.VERTICAL, // orientation
				true, // include legend
				true, // tooltips
				false // urls
				);

		// NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
		// final StandardLegend legend = (StandardLegend) chart.getLegend();
		// legend.setDisplaySeriesShapes(true);
		// legend.setShapeScaleX(1.5);
		// legend.setShapeScaleY(1.5);
		// legend.setDisplaySeriesLines(true);

		chart.setBackgroundPaint(Color.white);

		final CategoryPlot plot = (CategoryPlot) chart.getPlot();
		plot.setBackgroundPaint(Color.lightGray);
		plot.setRangeGridlinePaint(Color.white);

		// customise the range axis...
		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		// rangeAxis.setAutoRange(true);
		// rangeAxis.setRange(range);
		// setFixedAutoRange(20);
		// setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		// rangeAxis.setAutoRangeIncludesZero(true);

		// ****************************************************************************
		// * JFREECHART DEVELOPER GUIDE *
		// * The JFreeChart Developer Guide, written by David Gilbert, is
		// available *
		// * to purchase from Object Refinery Limited: *
		// * *
		// * http://www.object-refinery.com/jfreechart/guide.html *
		// * *
		// * Sales are used to provide funding for the JFreeChart project -
		// please *
		// * support us so that we can continue developing free software. *
		// ****************************************************************************

		// customise the renderer...
		final LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot
				.getRenderer();
		// renderer.setDrawShapes(true);

		renderer.setSeriesStroke(0, new BasicStroke(2.0f,
				BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f,
				new float[] { 10.0f, 6.0f }, 0.0f));
		renderer.setSeriesStroke(1, new BasicStroke(2.0f,
				BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f,
				new float[] { 6.0f, 6.0f }, 0.0f));
		renderer.setSeriesStroke(2, new BasicStroke(2.0f,
				BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f,
				new float[] { 2.0f, 6.0f }, 0.0f));
		// OPTIONAL CUSTOMISATION COMPLETED.

		return chart;

	}

	public static void main(String[] args) throws Exception {
		getDimensionValues();

		TimeSeries demo = new TimeSeries("countryCode=BR");
		demo.pack();
		RefineryUtilities.centerFrameOnScreen(demo);
		demo.setVisible(true);

	}
}
