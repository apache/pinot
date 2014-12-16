package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.apache.helix.HelixAdmin;

import java.io.PrintWriter;
import java.util.Collection;

public class ThirdEyeRebalanceTask extends Task
{
  private final String clusterName;
  private final HelixAdmin helixAdmin;

  public ThirdEyeRebalanceTask(String clusterName, HelixAdmin helixAdmin)
  {
    super("rebalance");
    this.clusterName = clusterName;
    this.helixAdmin = helixAdmin;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    if (params.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection and replicas");
    }

    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }

    String collection = collectionParam.iterator().next();
    Collection<String> replicasParam = params.get("replicas");
    if (replicasParam == null || replicasParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide replicas");
    }

    int replicas = Integer.parseInt(replicasParam.iterator().next());
    printWriter.println("Rebalancing " + collection + " with " + replicas + " replicas...");
    printWriter.flush();
    helixAdmin.rebalance(clusterName, collection, replicas);
    printWriter.println("Done rebalancing");
    printWriter.flush();
  }
}
