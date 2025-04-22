package org.apache.pinot.controller.workload;

import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PropagationTest {

    private PinotHelixResourceManager _pinotHelixResourceManager;

    @BeforeClass
    public void setUp() {
        // Initialize the PinotHelixResourceManager or mock it as needed
        _pinotHelixResourceManager = Mockito.mock(PinotHelixResourceManager.class);
    }

    @Test
    public void getTableToHelixTagsTest() {

    }
}
