package org.apache.pinot.broker.routing.adaptiveserverselector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.pinot.broker.routing.instanceselector.SegmentInstanceCandidate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class PriorityGroupInstanceSelectorTest {

  private AdaptiveServerSelector _adaptiveServerSelector;
  private PriorityGroupInstanceSelector _selector;
  private ServerSelectionContext _context;

  @BeforeMethod
  public void setUp() {
    _adaptiveServerSelector = mock(AdaptiveServerSelector.class);
    _selector = new PriorityGroupInstanceSelector(_adaptiveServerSelector);
    _context = mock(ServerSelectionContext.class);
  }

  @Test
  public void testSelectWithEmptyCandidates() {
    Optional<SegmentInstanceCandidate> result = _selector.select(_context, Collections.emptyList());
    assertFalse(result.isPresent());
  }

  @Test
  public void testSelectWithNullCandidates() {
    Optional<SegmentInstanceCandidate> result = _selector.select(_context, null);
    assertFalse(result.isPresent());
  }

  @Test
  public void testSelectWithNoPreferredGroups() {
    // Setup
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server1", 1),
        createCandidate("server2", 2),
        createCandidate("server3", 3)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Collections.emptyList());
    when(_adaptiveServerSelector.select(any())).thenReturn("server2");

    // Execute
    Optional<SegmentInstanceCandidate> result = _selector.select(_context, candidates);

    // Verify
    assertTrue(result.isPresent());
    assertEquals(result.get().getInstance(), "server2");
  }

  @Test
  public void testSelectWithPreferredGroupHasServers() {
    // Setup - Example 1 from Javadoc
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server1", 1),
        createCandidate("server2", 2),
        createCandidate("server3", 1)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Arrays.asList(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server2");

    // Execute
    Optional<SegmentInstanceCandidate> result = _selector.select(_context, candidates);

    // Verify
    assertTrue(result.isPresent());
    assertEquals(result.get().getInstance(), "server2");
    assertEquals(result.get().getReplicaGroup(), 2);
  }

  @Test
  public void testSelectWithFallbackToSecondPreferredGroup() {
    // Setup - Example 2 from Javadoc
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server1", 1),
        createCandidate("server3", 1),
        createCandidate("server4", 3)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Arrays.asList(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server1");

    // Execute
    Optional<SegmentInstanceCandidate> result = _selector.select(_context, candidates);

    // Verify
    assertTrue(result.isPresent());
    assertEquals(result.get().getInstance(), "server1");
    assertEquals(result.get().getReplicaGroup(), 1);
  }

  @Test
  public void testSelectWithFallbackToNonPreferredGroup() {
    // Setup - Example 3 from Javadoc
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server4", 3),
        createCandidate("server5", 3)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Arrays.asList(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server4");

    // Execute
    Optional<SegmentInstanceCandidate> result = _selector.select(_context, candidates);

    // Verify
    assertTrue(result.isPresent());
    assertEquals(result.get().getInstance(), "server4");
    assertEquals(result.get().getReplicaGroup(), 3);
  }

  private SegmentInstanceCandidate createCandidate(String instance, int replicaGroup) {
    SegmentInstanceCandidate candidate = mock(SegmentInstanceCandidate.class);
    when(candidate.getInstance()).thenReturn(instance);
    when(candidate.getReplicaGroup()).thenReturn(replicaGroup);
    return candidate;
  }
}
