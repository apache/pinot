package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeBulkLoader;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeBulkLoaderAvroImpl;
import io.dropwizard.servlets.tasks.Task;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class ThirdEyeBulkLoadTask extends Task
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeBulkLoadTask.class);
  private static final String TYPE = "type";
  private static final String AVRO = "avro";
  private static final String ACTION = "action";
  private static final String ACTION_EXECUTE = "execute";
  private static final String ACTION_CLEANUP = "cleanup";

  private final ExecutorService executorService;
  private final StarTreeManager manager;
  private final File rootDir;
  private final File tmpDir;

  public ThirdEyeBulkLoadTask(ExecutorService executorService,
                              StarTreeManager manager,
                              File rootDir,
                              File tmpDir)
  {
    super("bulkLoad");
    this.executorService = executorService;
    this.manager = manager;
    this.rootDir = rootDir;
    this.tmpDir = tmpDir;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    // Get collection
    Collection<String> actionParam = params.get("action");
    if (actionParam == null || actionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide action (execute|cleanup)");
    }
    String action = actionParam.iterator().next();

    // Get collection
    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }
    String collection = collectionParam.iterator().next();

    // Get file type
    String type = AVRO;
    if (!params.get(TYPE).isEmpty())
    {
      type = params.get(TYPE).iterator().next();
    }

    if (ACTION_EXECUTE.equals(action))
    {
      doExecute(collection, type, printWriter);
    }
    else if (ACTION_CLEANUP.equals(action))
    {
      doCleanup(collection, printWriter);
    }
    else
    {
      throw new IllegalArgumentException("Unsupported action " + action);
    }
  }

  private void doCleanup(String collection, PrintWriter printWriter) throws IOException
  {
    File collectionDir = new File(tmpDir, collection);
    FileUtils.forceDelete(collectionDir);
    printWriter.println("Deleted " + collectionDir);
    printWriter.flush();
  }

  private void doExecute(String collection, String type, PrintWriter printWriter) throws IOException
  {
    // Get star tree
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No star tree for collection " + collection);
    }

    // Construct bulk loader
    StarTreeBulkLoader bulkLoader;
    if (AVRO.equals(type))
    {
      bulkLoader = new StarTreeBulkLoaderAvroImpl(executorService, printWriter);
    }
    else
    {
      throw new IllegalArgumentException("Invalid file type " + type);
    }

    // Load data
    File collectionRootDir = new File(rootDir, starTree.getConfig().getCollection());
    File collectionTmpDir = new File(tmpDir, starTree.getConfig().getCollection());
    bulkLoader.bulkLoad(starTree, collectionRootDir, collectionTmpDir);
  }
}
