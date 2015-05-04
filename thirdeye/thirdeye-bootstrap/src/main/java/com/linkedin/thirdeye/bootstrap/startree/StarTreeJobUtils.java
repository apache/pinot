package com.linkedin.thirdeye.bootstrap.startree;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.DimensionKey;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StarTreeJobUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeJobUtils.class);
  private static final String ENCODING = "UTF-8";

  public static String getTreeId(FileSystem fileSystem, Path treePath) throws Exception
  {
    ObjectInputStream inputStream = null;
    try
    {
      inputStream = new ObjectInputStream(fileSystem.open(treePath));
      StarTreeNode root = (StarTreeNode) inputStream.readObject();
      return root.getId().toString();
    }
    finally
    {
      if (inputStream != null)
      {
        inputStream.close();
      }
    }
  }

  public static int pushConfig(InputStream configData, String thirdEyeUri, String collection) throws IOException
  {
    String url = thirdEyeUri + "/collections/" + URLEncoder.encode(collection, ENCODING);
    return executeHttpPost(configData, url);
  }

  public static int pushTree(InputStream treeData,
                             String thirdEyeUri,
                             String collection,
                             String treeId,
                             DateTime minTime,
                             DateTime maxTime,
                             String schedule) throws IOException
  {
    String url = thirdEyeUri + "/collections/" + URLEncoder.encode(collection, ENCODING)
            + "/starTree/" + URLEncoder.encode(treeId, ENCODING)
            + "/" + minTime.getMillis()
            + "/" + maxTime.getMillis();

    if (schedule != null)
    {
      url += "?schedule=" + URLEncoder.encode(schedule, ENCODING);
    }

    return executeHttpPost(treeData, url);
  }

  /**
   * POSTs a tar.gz archive to {thirdEyeUri}/collections/{collection}/data
   *
   * @return
   *  The status code of the HTTP response
   */
  public static int pushData(InputStream leafData,
                             String thirdEyeUri,
                             String collection,
                             DateTime minTime,
                             DateTime maxTime,
                             String schedule) throws IOException
  {
    String url = thirdEyeUri + "/collections/" + URLEncoder.encode(collection, ENCODING) + "/data/"
            + minTime.getMillis() + "/"
            + maxTime.getMillis();

    if (schedule != null)
    {
      url += "?schedule=" + URLEncoder.encode(schedule, ENCODING);
    }

    LOG.info("POST {}", url);

    return executeHttpPost(leafData, url);
  }

  private static int executeHttpPost(InputStream data, String url) throws IOException
  {
    HttpURLConnection http = (HttpURLConnection) new URL(url).openConnection();
    http.setChunkedStreamingMode(1024 * 1024);

    http.setRequestMethod("POST");
    http.setRequestProperty("Content-Type", "application/octet-stream");
    http.setDoOutput(true);
    IOUtils.copy(data, http.getOutputStream());
    http.getOutputStream().flush();
    http.getOutputStream().close();

    return http.getResponseCode();
  }

  /**
   * Traverses tree structure and collects all combinations of record that are present (star/specific)
   */
  public static void collectRecords(StarTreeConfig config, StarTreeNode node, StarTreeRecord record, Map<UUID, StarTreeRecord> collector)
  {
    if (node.isLeaf())
    {
      collector.put(node.getId(), record);
    }
    else
    {
      StarTreeNode target = node.getChild(record.getDimensionKey().getDimensionValue(config.getDimensions(), node.getChildDimensionName()));
      if (target == null)
      {
        target = node.getOtherNode();
        record = record.aliasOther(target.getDimensionName());
      }
      collectRecords(config, target, record, collector);
      collectRecords(config, node.getStarNode(), record.relax(target.getDimensionName()), collector);
    }
  }

  /**
   * Given a fixed list of combinations, finds the combination with fewest "other" values that dimensionKey
   * maps to, and returns the integer representation of that combination.
   */
  public static int[] findBestMatch(DimensionKey dimensionKey,
                                    List<String> dimensionNames,
                                    List<int[]> dimensionCombinations,
                                    Map<String, Map<String, Integer>> forwardIndex)
  {
    // Convert dimension key
    int[] target = new int[dimensionKey.getDimensionValues().length];
    for (int i = 0; i < dimensionNames.size(); i++)
    {
      String dimensionName = dimensionNames.get(i);
      String dimensionValue = dimensionKey.getDimensionValues()[i];

      Integer intValue = forwardIndex.get(dimensionName).get(dimensionValue);
      if (intValue == null)
      {
        //TODO: this check is only valid for dimensions that are already split.
       // throw new IllegalArgumentException("No mapping for " + dimensionName + ":" + dimensionValue + " in index");

        intValue = -1;
      }

      target[i] = intValue;
    }

    // Find node with least others
    int[] closestCombination = null;
    Integer closestScore = null;
    for (int[] combination : dimensionCombinations)
    {
      int score = 0;
      for (int i = 0; i < target.length; i++)
      {
        if (target[i] != combination[i])
        {
          if (combination[i] == StarTreeConstants.OTHER_VALUE)
          {
            score += 1;
          }
          else if (combination[i] == StarTreeConstants.STAR_VALUE)
          {
            score += 0;
          }
          else if(target[i] == StarTreeConstants.STAR_VALUE)
          {
            score += 0;
          }
          else
          {
            score = -1;
            break;
          }
        }
        // else, match and contribute 0
      }
      if (score >= 0 && (closestScore == null || score < closestScore))
      {
        closestScore = score;
        closestCombination = combination;
      }
    }

    // Check
    if (closestCombination == null)
    {
      StringBuilder sb = new StringBuilder();
      for(int[] combination:dimensionCombinations){
        sb.append(Arrays.toString(combination));
        sb.append("\n");
      }
      throw new IllegalArgumentException("Could not find matching combination for " + dimensionKey + " in \n" + sb.toString() +"\n"+ " forwardIndex:"+ forwardIndex);
    }

    return closestCombination;
  }
}
