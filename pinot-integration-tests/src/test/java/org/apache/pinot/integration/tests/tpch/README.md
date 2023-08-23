# How to run advanced TPCHQueryIntegrationTest
## enrich data
1. Follow https://dev.mysql.com/doc/heatwave/en/mys-hw-tpch-sample-data.html to generate tpch data.
The current tpch dataset in the `resource` folder was generated using scale factor 0.001, which is too small for the advanced query test.
```
./dbgen -vf -s 0.001
```
2. Run TblToAvro to convert the data to avro format. Remember to provide the absolute folder path of those `tbl` files.
3. Copy those replace the current dataset in the `resource` folder.
```
export TPCH_AVRO_FILE_SOURCE_FOLDER={the absolute folder path of tbl files}
export PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER={the absolute folder path of pinot tpch integration test source folder
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/customer.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/lineitem.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/nation.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/orders.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/part.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/partsupp.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/region.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/supplier.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
```
