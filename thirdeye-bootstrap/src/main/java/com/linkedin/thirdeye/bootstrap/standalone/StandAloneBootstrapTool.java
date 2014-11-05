package com.linkedin.thirdeye.bootstrap.standalone;

import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Takes as input raw star tree record streams, and constructs a tree + fixed leaf buffers.
 */
public class StandAloneBootstrapTool implements Runnable
{
  private final Collection<Iterable<StarTreeRecord>> recordStreams;
  private final File outputDir;
  private final StarTreeRecordThresholdFunction thresholdFunction;

  public StandAloneBootstrapTool(Collection<Iterable<StarTreeRecord>> recordStreams,
                                 File outputDir,
                                 StarTreeRecordThresholdFunction thresholdFunction)
  {
    this.recordStreams = recordStreams;
    this.outputDir = outputDir;
    this.thresholdFunction = thresholdFunction;
  }

  @Override
  public void run()
  {
    throw new UnsupportedOperationException("TODO");
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length < 4)
    {
      throw new IllegalArgumentException("usage: config.json fileType outputDir inputFile ...");
    }

    // Parse args
    String configJson = args[0];
    String fileType = args[1];
    String outputDir = args[2];
    String[] inputFiles = Arrays.copyOfRange(args, 3, args.length);

    // Parse config
    JsonNode config = new ObjectMapper().readTree(new File(configJson));

    // Get dimension names
    List<String> dimensionNames = new ArrayList<String>();
    for (JsonNode dimensionName : config.get("dimensionNames"))
    {
      dimensionNames.add(dimensionName.asText());
    }

    // Get metric names
    List<String> metricNames = new ArrayList<String>();
    for (JsonNode metricName : config.get("metricNames"))
    {
      metricNames.add(metricName.asText());
    }

    // Get time column name
    String timeColumnName = config.get("timeColumnName").asText();

    // Construct threshold function
    StarTreeRecordThresholdFunction thresholdFunction = null;
    if (config.has("thresholdFunctionClass"))
    {
      thresholdFunction = (StarTreeRecordThresholdFunction) Class.forName(config.get("thresholdFunctionClass").asText()).newInstance();
      if (config.has("thresholdFunctionConfig"))
      {
        Properties props = new Properties();
        Iterator<Map.Entry<String, JsonNode>> itr = config.get("thresholdFunctionConfig").getFields();
        while (itr.hasNext())
        {
          Map.Entry<String, JsonNode> next = itr.next();
          props.put(next.getKey(), next.getValue().asText());
        }
        thresholdFunction.init(props);
      }
    }

    // Construct record streams
    List<Iterable<StarTreeRecord>> recordStreams = new ArrayList<Iterable<StarTreeRecord>>();
    if ("avro".equals(fileType))
    {
      for (String inputFile : inputFiles)
      {
        recordStreams.add(new StarTreeRecordStreamAvroFileImpl(
                new File(inputFile),
                dimensionNames,
                metricNames,
                timeColumnName));
      }
    }
    else
    {
      throw new IllegalArgumentException("Invalid file type " + fileType);
    }

    // Run bootstrap job
    new StandAloneBootstrapTool(recordStreams, new File(outputDir), thresholdFunction).run();
  }
}
