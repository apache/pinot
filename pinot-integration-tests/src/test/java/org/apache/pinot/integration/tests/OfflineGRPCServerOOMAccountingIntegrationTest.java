package org.apache.pinot.integration.tests;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public class OfflineGRPCServerOOMAccountingIntegrationTest extends OfflineGRPCServerIntegrationTest {
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
            + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, true);
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED, true);
  }
}
