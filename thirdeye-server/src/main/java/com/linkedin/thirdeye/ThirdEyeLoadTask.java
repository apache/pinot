package com.linkedin.thirdeye;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTreeManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.Collection;

public class ThirdEyeLoadTask extends Task
{
  private final StarTreeManager manager;
  private final File dataRoot;

  public ThirdEyeLoadTask(StarTreeManager manager, File dataRoot)
  {
    super("load");
    this.manager = manager;
    this.dataRoot = dataRoot;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }

    String collection = collectionParam.iterator().next();

    File collectionRoot = new File(dataRoot, collection);
    File configFile = new File(collectionRoot, "config.json");
    File treeFile = new File(collectionRoot, "tree.bin");

    printWriter.println("Loading data for " + collection + "...");

    manager.restore(collection,
                    new FileInputStream(treeFile),
                    new FileInputStream(configFile));

    printWriter.println("Done!");
  }
}
