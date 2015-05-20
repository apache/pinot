package com.linkedin.pinot.controller.api.reslet.resources.v2;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


/**
 * "resourceName":"adsEvents",
 * "resourceType":"REALTIME",
 * "tableName":"adsEvents",
 * "timeColumnName":"daysSinceEpoch",
 * "timeType":"daysSinceEpoch",
 * "numberOfDataInstances":3,
 * "numberOfCopies":3,
 * "retentionTimeUnit":"DAYS",
 * "retentionTimeValue":"7",
 * "pushFrequency":"daily",
 * "brokerTagName":"colocatedBroker",
 * "numberOfBrokerInstances":3,
 * "segmentAssignmentStrategy":"BalanceNumSegmentAssignmentStrategy",
 "streamType":"kafka",
 "stream.kafka.consumer.type":"highLevel",
 "stream.kafka.topic.name":"ScinActivityEvent",
 "stream.kafka.decoder.class.name":"com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder",
 "stream.kafka.hlc.zk.connect.string":"zk-lva1-kafka.prod.linkedin.com:12913/kafka-aggregate-tracking",
 "stream.kafka.decoder.prop.schema.registry.rest.url":"http://ela4-schema-registry-vip-1.prod.linkedin.com:10252/schemaRegistry/schemas",
 */

/**
 *
 *
 * Offline Table request
 curl -i -X POST -H 'Content-Type: application/json' -d '{
    "tableName":"xlntBeta",
    "segmentsConfig" : {
        "retentionTimeUnit":"DAYS",
        "retentionTimeValue":"700",
        "segmentPushFrequency":"daily",
        "segmentPushType":"APPEND",
        "replication" : "3",
        "schemaName" : "tableSchema"
    },
    "tableIndexConfig" : {
        "invertedIndexColumns" : ["column1","column2"],
        "loadMode"  : "HEAP",
        "lazyLoad"  : "false"
    },
    "tenants" : {
        "broker":"brokerOne",
        "server":"serverOne"
    },
    "tableType":"OFFLINE",
    "metadata": {
        customConfigs : {
            "d2Name":"xlntBetaPinot"
        }
    }
}' http://localhost:9000/tables

 */
public class PinotTableRestletResource extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableRestletResource() throws IOException {
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
    baseDataDir = new File(conf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Override
  @Post("json")
  public Representation post(Representation entity) {
    StringRepresentation presentation = null;

    try {
      String jsonRequest = entity.getText();
      AbstractTableConfig config = AbstractTableConfig.init(jsonRequest);
      return new StringRepresentation(config.toString());

    } catch (Exception e) {
      LOGGER.error("error reading requestJSON", e);
    }

    try {

    } catch (Exception e) {

    }

    return null;
  }
}
