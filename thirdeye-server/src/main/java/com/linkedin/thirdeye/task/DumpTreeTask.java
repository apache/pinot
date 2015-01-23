package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

public class DumpTreeTask extends Task
{
  private final StarTreeManager manager;

  public DumpTreeTask(StarTreeManager manager)
  {
    super("dumpTree");
    this.manager = manager;
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

    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No star tree for collection " + collection);
    }

    StarTreeUtils.printNode(printWriter, starTree.getRoot(), 0);

    printWriter.flush();
  }
}
