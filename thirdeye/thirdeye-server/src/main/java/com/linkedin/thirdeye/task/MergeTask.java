package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import io.dropwizard.servlets.tasks.Task;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MergeTask extends Task {
  private final StarTreeManager starTreeManager;
  private final DataUpdateManager dataUpdateManager;

  public MergeTask(StarTreeManager starTreeManager, DataUpdateManager dataUpdateManager) {
    super("merge");
    this.starTreeManager = starTreeManager;
    this.dataUpdateManager = dataUpdateManager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
    for (String collection : params.get("collection")) {
      printWriter.println("NOTE: WILL ONLY MERGE data_KAFKA* trees currently!");
      printWriter.println("Merging trees for " + collection);
      printWriter.flush();

      Map<File, StarTree> toMerge = new HashMap<>();

      for (Map.Entry<File, StarTree> entry : starTreeManager.getStarTrees(collection).entrySet()) {
        File dataDir = entry.getKey();
        if (dataDir.getName().startsWith("data_KAFKA")) {
          printWriter.println("Will merge " + dataDir);
          printWriter.flush();
          toMerge.put(entry.getKey(), entry.getValue());
        }
      }

      // Determine min / max (wall-clock) time of the merged trees
      DateTime minTime = null;
      DateTime maxTime = null;
      for (File dataDir : toMerge.keySet()) {
        String[] tokens = dataDir.getName().split("_");
        DateTime segmentMinTime = StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(tokens[2]);
        DateTime segmentMaxTime = StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(tokens[3]);

        if (minTime == null || segmentMinTime.compareTo(minTime) < 0) {
          minTime = segmentMinTime.toDateTime(DateTimeZone.UTC);
        }

        if (maxTime == null || segmentMaxTime.compareTo(maxTime) > 0) {
          maxTime = segmentMaxTime.toDateTime(DateTimeZone.UTC);
        }
      }

      printWriter.println("Min time for merged tree " + minTime);
      printWriter.println("Max time for merged tree " + maxTime);
      printWriter.flush();

      // Merge the trees
      printWriter.println("Merging...");
      printWriter.flush();
      StarTree mergedTree = dataUpdateManager.mergeTrees(toMerge.values());

      // n.b. !!! This has a race condition in which we may double count values in the merged segments,
      // but which will eventually correct. To ensure the query path is unaffected, we need to prefer
      // to query segments of a more coarse time granularity

      // Persist the merged tree
      printWriter.println("Persisting merged tree...");
      printWriter.flush();
      dataUpdateManager.persistTree(collection, "KAFKA-MERGED", minTime, maxTime, mergedTree);

      // Remove the existing ones
      printWriter.println("Removing original trees...");
      printWriter.flush();
      for (File dataDir : toMerge.keySet()) {
        printWriter.println("Removing " + dataDir);
        printWriter.flush();
        FileUtils.forceDelete(dataDir);
      }

      printWriter.println("Done!");
      printWriter.flush();
    }
  }
}
