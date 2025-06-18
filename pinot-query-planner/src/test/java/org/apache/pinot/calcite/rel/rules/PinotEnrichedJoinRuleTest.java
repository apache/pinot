package org.apache.pinot.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.pinot.query.type.TypeFactory;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;


public class PinotEnrichedJoinRuleTest {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  private AutoCloseable _mocks;

  @Mock
  private RelOptRuleCall _call;
  @Mock
  private RelNode _input;
  @Mock
  private RelOptCluster _cluster;
  @Mock
  private RelMetadataQuery _query;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    RelTraitSet traits = RelTraitSet.createEmpty();
    Mockito.when(_input.getTraitSet()).thenReturn(traits);
    Mockito.when(_input.getCluster()).thenReturn(_cluster);
    Mockito.when(_call.getMetadataQuery()).thenReturn(_query);
    Mockito.when(_query.getMaxRowCount(Mockito.any())).thenReturn(null);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  // TODO: test
}
