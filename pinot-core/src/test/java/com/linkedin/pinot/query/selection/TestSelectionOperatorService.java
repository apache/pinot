package com.linkedin.pinot.query.selection;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestSelectionOperatorService {

  private static BrokerRequest _brokerRequest;
  private static IndexSegment _indexSegment;
  private static IndexSegment _indexSegment2;
  private static IndexSegment _indexSegmentWithSchema1;
  private static IndexSegment _indexSegmentWithSchema2;
  private static int _indexSize = 20000001;

  @BeforeClass
  public static void setup() {
    _brokerRequest = getSelectionNoFilterBrokerRequest();
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(_indexSize);
    _indexSegment2 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(_indexSize);
    final Schema schema1 = new Schema();
    schema1.addSchema("dim0", new FieldSpec("dim0", FieldType.dimension, DataType.DOUBLE, true));
    schema1.addSchema("dim1", new FieldSpec("dim1", FieldType.dimension, DataType.DOUBLE, true));
    schema1.addSchema("met", new FieldSpec("met", FieldType.metric, DataType.DOUBLE, true));
    final Schema schema2 = new Schema();
    schema2.addSchema("dim0", new FieldSpec("dim0", FieldType.dimension, DataType.DOUBLE, true));
    schema2.addSchema("dim1", new FieldSpec("dim1", FieldType.dimension, DataType.DOUBLE, true));
    schema2.addSchema("met", new FieldSpec("met", FieldType.metric, DataType.DOUBLE, true));

    _indexSegmentWithSchema1 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, schema1);
    _indexSegmentWithSchema2 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, schema2);

  }

  @Test
  public void testEmptySelectionService() {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    final SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);
    Assert.assertEquals(selectionOperatorService.getNumDocsScanned(), 0);
    final PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService.getRowEventsSet();
    assertEquals(true, rowEventsSet.isEmpty());
  }

  @Test
  public void testMapDocSelectionService() {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().getSelectionSortSequence().get(0).setIsAsc(false);
    final SelectionOperatorService selectionService =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);

    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(_indexSize);
    blockValSets[1] = getDim1BlockValSet(_indexSize);
    blockValSets[2] = getMetBlockValSet(_indexSize);
    selectionService.iterateOnBlock(getBlockDocIdIterator(_indexSize), blockValSets);

    Assert.assertEquals(selectionService.getNumDocsScanned(), _indexSize);
    final PriorityQueue<Serializable[]> rowEventsSet = selectionService.getRowEventsSet();

    System.out.println(selectionService.getDataSchema().toString());
    while (!rowEventsSet.isEmpty()) {
      final Serializable[] rowSerializables = rowEventsSet.poll();
      Assert.assertEquals(((Double) rowSerializables[0]).doubleValue(), 9.0);
      Assert.assertEquals(((Double) rowSerializables[1]).doubleValue(), 99.0);
      System.out.println(Arrays.toString(rowSerializables));
    }
  }

  @Test
  public void testMergeInSelectionService() {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();

    final SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);

    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(40);
    blockValSets[1] = getDim1BlockValSet(40);
    blockValSets[2] = getMetBlockValSet(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValSets);

    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();

    final SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment2);

    final BlockValSet[] blockValSets2 = new BlockValSet[3];
    blockValSets2[0] = getDim0BlockValSet(40);
    blockValSets2[1] = getDim1BlockValSet(40);
    blockValSets2[2] = getMetBlockValSet(40);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40), blockValSets2);
    final PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();

    final PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService1.merge(rowEventsSet1, rowEventsSet2);
    System.out.println(selectionOperatorService1.getDataSchema().toString());
    while (!rowEventsSet.isEmpty()) {
      final Serializable[] rowSerializables = rowEventsSet.poll();
      System.out.println(Arrays.toString(rowSerializables));
    }
  }

  @Test
  public void testToDataTable() throws Exception {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();

    final SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);

    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(40);
    blockValSets[1] = getDim1BlockValSet(40);
    blockValSets[2] = getMetBlockValSet(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValSets);

    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    final SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment2);

    final BlockValSet[] blockValSets2 = new BlockValSet[3];
    blockValSets2[0] = getDim0BlockValSet(40, 80);
    blockValSets2[1] = getDim1BlockValSet(40, 80);
    blockValSets2[2] = getMetBlockValSet(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValSets2);

    final PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    final DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    final DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    final Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    final ReduceService reduceService = new DefaultReduceService();
    final BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    System.out.println(brokerResponse);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());

  }

  @Test
  public void testRender() throws Exception {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();

    final SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);
    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(40);
    blockValSets[1] = getDim1BlockValSet(40);
    blockValSets[2] = getMetBlockValSet(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValSets);
    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    final SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment2);
    final BlockValSet[] blockValSets2 = new BlockValSet[3];
    blockValSets2[0] = getDim0BlockValSet(40, 80);
    blockValSets2[1] = getDim1BlockValSet(40, 80);
    blockValSets2[2] = getMetBlockValSet(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValSets2);
    final PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    final DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    final DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    final Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    final ReduceService reduceService = new DefaultReduceService();
    final BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());

  }

  @Test
  public void testSelectionStar() throws Exception {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    final List<String> selectionColumns = brokerRequest.getSelections().getSelectionColumns();
    selectionColumns.clear();
    selectionColumns.add("*");

    final SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(40);
    blockValSets[1] = getDim1BlockValSet(40);
    blockValSets[2] = getMetBlockValSet(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValSets);
    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    final SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    final BlockValSet[] blockValSets2 = new BlockValSet[3];
    blockValSets2[0] = getDim0BlockValSet(40, 80);
    blockValSets2[1] = getDim1BlockValSet(40, 80);
    blockValSets2[2] = getMetBlockValSet(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValSets2);
    final PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    final DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    final DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    final Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    final ReduceService reduceService = new DefaultReduceService();
    final BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());
  }

  @Test
  public void testSelectionStarWithoutOrdering() throws Exception {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    final List<String> selectionColumns = brokerRequest.getSelections().getSelectionColumns();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    selectionColumns.clear();
    selectionColumns.add("*");

    final SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(40);
    blockValSets[1] = getDim1BlockValSet(40);
    blockValSets[2] = getMetBlockValSet(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValSets);
    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    final SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    final BlockValSet[] blockValSets2 = new BlockValSet[3];
    blockValSets2[0] = getDim0BlockValSet(40, 80);
    blockValSets2[1] = getDim1BlockValSet(40, 80);
    blockValSets2[2] = getMetBlockValSet(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValSets2);
    final PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    final DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    final DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    final Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    final ReduceService reduceService = new DefaultReduceService();
    final BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());
  }

  private static BrokerRequest getSelectionNoFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    return brokerRequest;
  }

  private static Selection getSelectionQuery() {
    final Selection selection = new Selection();
    selection.setOffset(0);
    selection.setSize(80);
    final List<String> selectionColumns = new ArrayList<String>();
    selectionColumns.add("dim0");
    selectionColumns.add("dim1");
    selectionColumns.add("met");
    selection.setSelectionColumns(selectionColumns);

    final List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("dim0");
    selectionSort.setIsAsc(true);
    selectionSortSequence.add(selectionSort);
    selectionSort = new SelectionSort();
    selectionSort.setColumn("dim1");
    selectionSort.setIsAsc(false);
    selectionSortSequence.add(selectionSort);

    selection.setSelectionSortSequence(selectionSortSequence);

    return selection;
  }

  @Test
  public void testMapDocSelectionServiceNoOrdering() {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);

    final SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);

    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(_indexSize);
    blockValSets[1] = getDim1BlockValSet(_indexSize);
    blockValSets[2] = getMetBlockValSet(_indexSize);
    selectionOperatorService.iterateOnBlock(getBlockDocIdIterator(_indexSize), blockValSets);

    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService.getRowEventsSet();
    System.out.println("rowEventsSet.size() = " + rowEventsSet1.size());

    Assert.assertEquals(selectionOperatorService.getNumDocsScanned(), _indexSize);
    final PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService.getRowEventsSet();
    System.out.println(selectionOperatorService.getDataSchema().toString());
    int i = 79;
    while (!rowEventsSet.isEmpty()) {
      final Serializable[] row = rowEventsSet.poll();
      System.out.println(Arrays.toString(row));
      Assert.assertEquals(((Integer) row[1]).intValue(), i);
      Assert.assertEquals(((Double) row[2]).doubleValue(), (double) (i % 10));
      Assert.assertEquals(((Double) row[3]).doubleValue(), (double) i);
      Assert.assertEquals(((Double) row[4]).doubleValue(), (double) i);
      i--;
    }
    Assert.assertEquals(i, -1);
  }

  @Test
  public void testMergeInSelectionServiceNoOrdering() {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);

    final SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(40);
    blockValSets[1] = getDim1BlockValSet(40);
    blockValSets[2] = getMetBlockValSet(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValSets);
    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    final SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    final BlockValSet[] blockValSets2 = new BlockValSet[3];
    blockValSets2[0] = getDim0BlockValSet(40);
    blockValSets2[1] = getDim1BlockValSet(40);
    blockValSets2[2] = getMetBlockValSet(40);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40), blockValSets2);

    final PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    final PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService1.merge(rowEventsSet1, rowEventsSet2);
    System.out.println(selectionOperatorService1.getDataSchema().toString());
    int i = 39;
    while (!rowEventsSet.isEmpty()) {
      final Serializable[] rowSerializables = rowEventsSet.poll();
      Assert.assertEquals(((Integer) rowSerializables[1]).intValue(), i);
      Assert.assertEquals(((Double) rowSerializables[2]).doubleValue(), (double) (i % 10));
      Assert.assertEquals(((Double) rowSerializables[3]).doubleValue(), (double) i);
      Assert.assertEquals(((Double) rowSerializables[4]).doubleValue(), (double) i);
      i--;
      if (i < 0) {
        i += 40;
      }
      System.out.println(Arrays.toString(rowSerializables));
    }
    Assert.assertEquals(i, 39);
  }

  @Test
  public void testToDataTableNoOrdering() throws Exception {
    final BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(80);

    final SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    final BlockValSet[] blockValSets = new BlockValSet[3];
    blockValSets[0] = getDim0BlockValSet(50);
    blockValSets[1] = getDim1BlockValSet(50);
    blockValSets[2] = getMetBlockValSet(50);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(50), blockValSets);
    final PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    final SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);

    final BlockValSet[] blockValSets2 = new BlockValSet[3];
    blockValSets2[0] = getDim0BlockValSet(30);
    blockValSets2[1] = getDim1BlockValSet(30);
    blockValSets2[2] = getMetBlockValSet(30);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(30), blockValSets2);

    final PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    final DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    final DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    final Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    final ReduceService reduceService = new DefaultReduceService();
    final BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    System.out.println(brokerResponse);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    for (int i = 0; i < 80; ++i) {
      if (i < 50) {
        Assert
        .assertEquals(
            Integer.parseInt(brokerResponse.getSelectionResults().getJSONArray("results").getJSONArray(i)
                .getString(1)), i);
      } else {
        Assert
        .assertEquals(
            Integer.parseInt(brokerResponse.getSelectionResults().getJSONArray("results").getJSONArray(i)
                .getString(1)), i - 50);
      }
      Assert.assertEquals(
          Integer.parseInt(brokerResponse.getSelectionResults().getJSONArray("results").getJSONArray(i).getString(0)),
          i % 10);
    }
  }

  private BlockValSet getMetBlockValSet(int i) {
    return getMetBlockValSet(0, i);
  }

  private BlockValSet getDim1BlockValSet(int i) {
    return getDim1BlockValSet(0, i);
  }

  private BlockValSet getDim0BlockValSet(int i) {
    return getDim0BlockValSet(0, i);
  }

  private BlockValSet getMetBlockValSet(int i, int j) {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataType getValueType() {
        return DataType.INT;
      }
    };
  }

  private BlockValSet getDim1BlockValSet(int i, int j) {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataType getValueType() {
        return DataType.INT;
      }
    };
  }

  private BlockValSet getDim0BlockValSet(int i, int j) {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataType getValueType() {
        return DataType.INT;
      }
    };
  }

  private BlockDocIdIterator getBlockDocIdIterator(final int size) {
    return getBlockDocIdIterator(0, size);
  }

  private BlockDocIdIterator getBlockDocIdIterator(final int start, final int end) {
    return new BlockDocIdIterator() {
      private int _pos = start;

      @Override
      public int skipTo(int targetDocId) {
        return targetDocId;
      }

      @Override
      public int next() {
        if (_pos == end) {
          return Constants.EOF;
        }
        return _pos++;
      }

      @Override
      public int currentDocId() {
        return _pos;
      }
    };
  }

}
