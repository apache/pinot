package org.apache.pinot.integration.tests;

import org.testng.annotations.BeforeTest;


public class OfflineGRPCServerMultiStageOOMAccountingIntegrationTest extends OfflineGRPCServerOOMAccountingIntegrationTest {
  @BeforeTest
  void enableMultiStage() {
    setUseMultiStageQueryEngine(true);
  }
}
