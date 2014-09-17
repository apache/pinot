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
    Schema schema1 = new Schema();
    schema1.addSchema("dim0", new FieldSpec("dim0", FieldType.dimension, DataType.DOUBLE, true));
    schema1.addSchema("dim1", new FieldSpec("dim1", FieldType.dimension, DataType.DOUBLE, true));
    schema1.addSchema("met", new FieldSpec("met", FieldType.metric, DataType.DOUBLE, true));
    Schema schema2 = new Schema();
    schema2.addSchema("dim0", new FieldSpec("dim0", FieldType.dimension, DataType.DOUBLE, true));
    schema2.addSchema("dim1", new FieldSpec("dim1", FieldType.dimension, DataType.DOUBLE, true));
    schema2.addSchema("met", new FieldSpec("met", FieldType.metric, DataType.DOUBLE, true));

    _indexSegmentWithSchema1 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, schema1);
    _indexSegmentWithSchema2 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, schema2);

  }

  @Test
  public void testEmptySelectionService() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);
    Assert.assertEquals(selectionOperatorService.getNumDocsScanned(), 0);
    PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService.getRowEventsSet();
    assertEquals(true, rowEventsSet.isEmpty());
  }

  @Test
  public void testMapDocSelectionService() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().getSelectionSortSequence().get(0).setIsAsc(false);
    SelectionOperatorService selectionService =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(_indexSize);
    blockValIterators[1] = getDim1BlockValIterator(_indexSize);
    blockValIterators[2] = getMetBlockValIterator(_indexSize);
    selectionService.iterateOnBlock(getBlockDocIdIterator(_indexSize), blockValIterators);

    Assert.assertEquals(selectionService.getNumDocsScanned(), _indexSize);
    PriorityQueue<Serializable[]> rowEventsSet = selectionService.getRowEventsSet();

    System.out.println(selectionService.getDataSchema().toString());
    while (!rowEventsSet.isEmpty()) {
      Serializable[] rowSerializables = rowEventsSet.poll();
      Assert.assertEquals(((Double) rowSerializables[0]).doubleValue(), 9.0);
      Assert.assertEquals(((Double) rowSerializables[1]).doubleValue(), 99.0);
      System.out.println(Arrays.toString(rowSerializables));
    }
  }

  @Test
  public void testMergeInSelectionService() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();

    SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(40);
    blockValIterators[1] = getDim1BlockValIterator(40);
    blockValIterators[2] = getMetBlockValIterator(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();

    SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment2);
    BlockValIterator[] blockValIterators2 = new BlockValIterator[3];
    blockValIterators2[0] = getDim0BlockValIterator(40);
    blockValIterators2[1] = getDim1BlockValIterator(40);
    blockValIterators2[2] = getMetBlockValIterator(40);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators2);
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();

    PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService1.merge(rowEventsSet1, rowEventsSet2);
    System.out.println(selectionOperatorService1.getDataSchema().toString());
    while (!rowEventsSet.isEmpty()) {
      Serializable[] rowSerializables = rowEventsSet.poll();
      System.out.println(Arrays.toString(rowSerializables));
    }
  }

  @Test
  public void testToDataTable() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();

    SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(40);
    blockValIterators[1] = getDim1BlockValIterator(40);
    blockValIterators[2] = getMetBlockValIterator(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment2);
    BlockValIterator[] blockValIterators2 = new BlockValIterator[3];
    blockValIterators2[0] = getDim0BlockValIterator(40, 80);
    blockValIterators2[1] = getDim1BlockValIterator(40, 80);
    blockValIterators2[2] = getMetBlockValIterator(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValIterators2);
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    System.out.println(brokerResponse);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());

  }

  @Test
  public void testRender() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();

    SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(40);
    blockValIterators[1] = getDim1BlockValIterator(40);
    blockValIterators[2] = getMetBlockValIterator(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegment2);
    BlockValIterator[] blockValIterators2 = new BlockValIterator[3];
    blockValIterators2[0] = getDim0BlockValIterator(40, 80);
    blockValIterators2[1] = getDim1BlockValIterator(40, 80);
    blockValIterators2[2] = getMetBlockValIterator(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValIterators2);
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());

  }

  @Test
  public void testSelectionStar() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    List<String> selectionColumns = brokerRequest.getSelections().getSelectionColumns();
    selectionColumns.clear();
    selectionColumns.add("*");

    SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(40);
    blockValIterators[1] = getDim1BlockValIterator(40);
    blockValIterators[2] = getMetBlockValIterator(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    BlockValIterator[] blockValIterators2 = new BlockValIterator[3];
    blockValIterators2[0] = getDim0BlockValIterator(40, 80);
    blockValIterators2[1] = getDim1BlockValIterator(40, 80);
    blockValIterators2[2] = getMetBlockValIterator(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValIterators2);
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());
  }

  @Test
  public void testSelectionStarWithoutOrdering() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    List<String> selectionColumns = brokerRequest.getSelections().getSelectionColumns();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    selectionColumns.clear();
    selectionColumns.add("*");

    SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(40);
    blockValIterators[1] = getDim1BlockValIterator(40);
    blockValIterators[2] = getMetBlockValIterator(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    BlockValIterator[] blockValIterators2 = new BlockValIterator[3];
    blockValIterators2[0] = getDim0BlockValIterator(40, 80);
    blockValIterators2[1] = getDim1BlockValIterator(40, 80);
    blockValIterators2[2] = getMetBlockValIterator(40, 80);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40, 80), blockValIterators2);
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());
  }

  private static BrokerRequest getSelectionNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    return brokerRequest;
  }

  private static Selection getSelectionQuery() {
    Selection selection = new Selection();
    selection.setOffset(0);
    selection.setSize(80);
    List<String> selectionColumns = new ArrayList<String>();
    selectionColumns.add("dim0");
    selectionColumns.add("dim1");
    selectionColumns.add("met");
    selection.setSelectionColumns(selectionColumns);

    List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
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
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);

    SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(_indexSize);
    blockValIterators[1] = getDim1BlockValIterator(_indexSize);
    blockValIterators[2] = getMetBlockValIterator(_indexSize);
    selectionOperatorService.iterateOnBlock(getBlockDocIdIterator(_indexSize), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService.getRowEventsSet();
    System.out.println("rowEventsSet.size() = " + rowEventsSet1.size());

    Assert.assertEquals(selectionOperatorService.getNumDocsScanned(), _indexSize);
    PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService.getRowEventsSet();
    System.out.println(selectionOperatorService.getDataSchema().toString());
    int i = 79;
    while (!rowEventsSet.isEmpty()) {
      Serializable[] row = rowEventsSet.poll();
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
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);

    SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(40);
    blockValIterators[1] = getDim1BlockValIterator(40);
    blockValIterators[2] = getMetBlockValIterator(40);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    BlockValIterator[] blockValIterators2 = new BlockValIterator[3];
    blockValIterators2[0] = getDim0BlockValIterator(40);
    blockValIterators2[1] = getDim1BlockValIterator(40);
    blockValIterators2[2] = getMetBlockValIterator(40);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(40), blockValIterators2);
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    PriorityQueue<Serializable[]> rowEventsSet = selectionOperatorService1.merge(rowEventsSet1, rowEventsSet2);
    System.out.println(selectionOperatorService1.getDataSchema().toString());
    int i = 39;
    while (!rowEventsSet.isEmpty()) {
      Serializable[] rowSerializables = rowEventsSet.poll();
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
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(80);

    SelectionOperatorService selectionOperatorService1 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    BlockValIterator[] blockValIterators = new BlockValIterator[3];
    blockValIterators[0] = getDim0BlockValIterator(50);
    blockValIterators[1] = getDim1BlockValIterator(50);
    blockValIterators[2] = getMetBlockValIterator(50);
    selectionOperatorService1.iterateOnBlock(getBlockDocIdIterator(50), blockValIterators);
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionOperatorService1.getRowEventsSet();
    System.out.println("rowEventsSet1.size() = " + rowEventsSet1.size());

    SelectionOperatorService selectionOperatorService2 =
        new SelectionOperatorService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    BlockValIterator[] blockValIterators2 = new BlockValIterator[3];
    blockValIterators2[0] = getDim0BlockValIterator(30);
    blockValIterators2[1] = getDim1BlockValIterator(30);
    blockValIterators2[2] = getMetBlockValIterator(30);
    selectionOperatorService2.iterateOnBlock(getBlockDocIdIterator(30), blockValIterators2);
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionOperatorService2.getRowEventsSet();
    System.out.println("rowEventsSet2.size() = " + rowEventsSet2.size());

    DataTable dataTable1 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet1, selectionOperatorService1.getDataSchema());
    DataTable dataTable2 =
        SelectionOperatorService.transformRowSetToDataTable(rowEventsSet2, selectionOperatorService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
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

  private BlockValIterator getMetBlockValIterator(final int size) {
    return getMetBlockValIterator(0, size);
  }

  private BlockValIterator getMetBlockValIterator(final int start, final int end) {
    return new BlockValIterator() {
      private int _pos = start;

      @Override
      public int size() {
        return end - start;
      }

      @Override
      public boolean reset() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public int nextVal() {
        return _pos++;
      }

      @Override
      public String nextStringVal() {
        return (_pos++) + "";
      }

      @Override
      public long nextLongVal() {
        return _pos++;
      }

      @Override
      public int nextIntVal() {
        return _pos++;
      }

      @Override
      public float nextFloatVal() {
        return _pos++;
      }

      @Override
      public double nextDoubleVal() {
        return _pos++;
      }

      @Override
      public boolean hasNext() {
        if (_pos < end) {
          return true;
        }
        return false;
      }

      @Override
      public int currentValId() {
        return _pos;
      }

      @Override
      public int currentDocId() {
        return _pos;
      }

      @Override
      public int getIntVal(int docId) {
        return docId;
      }

      @Override
      public float getFloatVal(int docId) {
        return docId;
      }

      @Override
      public long getLongVal(int docId) {
        return docId;
      }

      @Override
      public double getDoubleVal(int docId) {
        return docId;
      }

      @Override
      public String getStringVal(int docId) {
        return docId + "";
      }

      @Override
      public int nextDictVal() {
        // TODO Auto-generated method stub
        return 0;
      }
    };

  }

  private BlockValIterator getDim0BlockValIterator(final int size) {
    return getDim0BlockValIterator(0, size);
  }

  private BlockValIterator getDim0BlockValIterator(final int start, final int end) {
    return new BlockValIterator() {
      private int _pos = start;

      @Override
      public int size() {
        return end - start;
      }

      @Override
      public boolean reset() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public int nextVal() {
        return (_pos++) % 10;
      }

      @Override
      public String nextStringVal() {
        return ((_pos++) % 10) + "";
      }

      @Override
      public long nextLongVal() {
        return (_pos++) % 10;
      }

      @Override
      public int nextIntVal() {
        return (_pos++) % 10;
      }

      @Override
      public float nextFloatVal() {
        return (_pos++) % 10;
      }

      @Override
      public double nextDoubleVal() {
        return (_pos++) % 10;
      }

      @Override
      public boolean hasNext() {
        if (_pos < end) {
          return true;
        }
        return false;
      }

      @Override
      public int currentValId() {
        return _pos;
      }

      @Override
      public int currentDocId() {
        return _pos;
      }

      @Override
      public int getIntVal(int docId) {
        return docId % 10;
      }

      @Override
      public float getFloatVal(int docId) {
        return docId % 10;
      }

      @Override
      public long getLongVal(int docId) {
        return docId % 10;
      }

      @Override
      public double getDoubleVal(int docId) {
        return docId % 10;
      }

      @Override
      public String getStringVal(int docId) {
        return (docId % 10) + "";
      }

      @Override
      public int nextDictVal() {
        // TODO Auto-generated method stub
        return 0;
      }
    };

  }

  private BlockValIterator getDim1BlockValIterator(final int size) {
    return getDim1BlockValIterator(0, size);
  }

  private BlockValIterator getDim1BlockValIterator(final int start, final int end) {
    return new BlockValIterator() {
      private int _pos = start;

      @Override
      public int size() {
        return end - start;
      }

      @Override
      public boolean reset() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public int nextVal() {
        return (_pos++) % 100;
      }

      @Override
      public String nextStringVal() {
        return ((_pos++) % 100) + "";
      }

      @Override
      public long nextLongVal() {
        return (_pos++) % 100;
      }

      @Override
      public int nextIntVal() {
        return (_pos++) % 100;
      }

      @Override
      public float nextFloatVal() {
        return (_pos++) % 100;
      }

      @Override
      public double nextDoubleVal() {
        return (_pos++) % 100;
      }

      @Override
      public boolean hasNext() {
        if (_pos < end) {
          return true;
        }
        return false;
      }

      @Override
      public int currentValId() {
        return _pos;
      }

      @Override
      public int currentDocId() {
        return _pos;
      }

      @Override
      public int getIntVal(int docId) {
        return docId % 100;
      }

      @Override
      public float getFloatVal(int docId) {
        return docId % 100;
      }

      @Override
      public long getLongVal(int docId) {
        return docId % 100;
      }

      @Override
      public double getDoubleVal(int docId) {
        return docId % 100;
      }

      @Override
      public String getStringVal(int docId) {
        return (docId % 100) + "";
      }

      @Override
      public int nextDictVal() {
        // TODO Auto-generated method stub
        return 0;
      }
    };

  }

}
