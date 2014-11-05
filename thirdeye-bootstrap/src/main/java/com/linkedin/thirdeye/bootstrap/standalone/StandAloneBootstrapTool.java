package com.linkedin.thirdeye.bootstrap.standalone;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Takes as input raw star tree record streams, and constructs a tree + fixed leaf buffers.
 */
public class StandAloneBootstrapTool implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(StandAloneBootstrapTool.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String S1_COMBINATIONS_FILE = "S1_COMBINATIONS.json";

  private final Collection<Iterable<StarTreeRecord>> recordStreams;
  private final File outputDir;
  private final StarTreeRecordThresholdFunction thresholdFunction;
  private final ExecutorService executorService;

  public StandAloneBootstrapTool(Collection<Iterable<StarTreeRecord>> recordStreams,
                                 File outputDir,
                                 StarTreeRecordThresholdFunction thresholdFunction)
  {
    this.recordStreams = recordStreams;
    this.outputDir = outputDir;
    this.thresholdFunction = thresholdFunction;
    this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public void run()
  {
    try
    {
      s1Combinations();
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  private void s1Combinations() throws Exception
  {
    final AtomicInteger numRecords = new AtomicInteger();
    final BlockingQueue<StarTreeRecord> queue = new ArrayBlockingQueue<StarTreeRecord>(100);
    final List<Future<Set<Map<String, String>>>> futures = new ArrayList<Future<Set<Map<String, String>>>>();

    // Workers
    for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++)
    {
      futures.add(executorService.submit(new Callable<Set<Map<String, String>>>()
      {
        @Override
        public Set<Map<String, String>> call() throws Exception
        {
          Set<Map<String, String>> combinations = new HashSet<Map<String, String>>();
          Map<String, String> choices = new HashMap<String, String>();

          StarTreeRecord record = null;
          try
          {
            while (!((record = queue.take()) instanceof StarTreeRecordEndMarker))
            {
              computeCombinations(record, choices, combinations);
              choices.clear();

              int n = numRecords.incrementAndGet();
              if (n % 10 == 0)
              {
                LOG.info("Processed {} records", n);
              }
            }
          }
          catch (InterruptedException e)
          {
            throw new RuntimeException(e);
          }

          return combinations;
        }
      }));
    }

    // Populate queue
    int streamId = 0;
    for (Iterable<StarTreeRecord> iterable : recordStreams)
    {
      LOG.info("Processing stream {} of {}", ++streamId, recordStreams.size());
      for (StarTreeRecord record : iterable)
      {
        queue.put(record);
      }
    }
    for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++)
    {
      queue.put(new StarTreeRecordEndMarker());
    }

    // Convert to list
    Set<Map<String, String>> results = new HashSet<Map<String, String>>();
    for (Future<Set<Map<String, String>>> future : futures)
    {
      results.addAll(future.get());
    }

    // Output to file
    File file = new File(outputDir, S1_COMBINATIONS_FILE);
    LOG.info("Writing {}", file);
    OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file, results);
  }

  private static class StarTreeRecordEndMarker extends StarTreeRecordImpl
  {
    StarTreeRecordEndMarker()
    {
      super(null, null, null);
    }
  }

  private void computeCombinations(StarTreeRecord record,
                                   Map<String, String> choices,
                                   Set<Map<String, String>> collector)
  {
    if (choices.size() == record.getDimensionValues().size())
    {
      // Copy map and add to collector
      if (!collector.contains(choices))
      {
        Map<String, String> copy = new HashMap<String, String>();
        copy.putAll(choices);
        collector.add(copy);
      }
    }
    else
    {
      for (Map.Entry<String, String> entry : record.getDimensionValues().entrySet())
      {
        if (!choices.containsKey(entry.getKey()))
        {
          // Explore specific
          choices.put(entry.getKey(), entry.getValue());
          computeCombinations(record, choices, collector);

          // Explore star
          choices.put(entry.getKey(), StarTreeConstants.STAR);
          computeCombinations(record, choices, collector);

          // Un-choose
          choices.remove(entry.getKey());
        }
      }
    }
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
    JsonNode config = OBJECT_MAPPER.readTree(new File(configJson));

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
