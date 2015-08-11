+ segmentMetadata.getIndexCreationTime() + " and crc " + segmentMetadata.getCrc();
LOGGER.info(msg);
res.status = ResponseStatus.success;
res.message = msg;
}
} else {
OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
offlineSegmentZKMetadata.setPushTime(System.currentTimeMillis());
ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
LOGGER.info("Added segment : " + offlineSegmentZKMetadata.getSegmentName() + " to Property store");

addNewOfflineSegment(segmentMetadata);
res.status = ResponseStatus.success;
}
} catch (final Exception e) {
LOGGER.error("Caught exception while adding segment", e);
}
}

/**
* Helper method to add the passed in offline segment to the helix cluster.
* - Gets the segment name and the table name from the passed in segment meta-data.
* - Identifies the instance set onto which the segment needs to be added, based on
*   segment assignment strategy and replicas in the table config in the property-store.
* - Updates ideal state such that the new segment is assigned to required set of instances as per
*    the segment assignment strategy and replicas.
*
* @param segmentMetadata Meta-data for the segment, used to access segmentName and tableName.
* @throws JsonParseException
* @throws JsonMappingException
* @throws JsonProcessingException
* @throws JSONException
* @throws IOException
*/
private void addNewOfflineSegment(final SegmentMetadata segmentMetadata) throws JsonParseException,
JsonMappingException, JsonProcessingException, JSONException, IOException {
final AbstractTableConfig offlineTableConfig =
ZKMetadataProvider.getOfflineTableConfig(_propertyStore, segmentMetadata.getTableName());

final String segmentName = segmentMetadata.getName();
final String offlineTableName =
TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName());

if (!SEGMENT_ASSIGNMENT_STRATEGY_MAP.containsKey(offlineTableName)) {
SEGMENT_ASSIGNMENT_STRATEGY_MAP.put(offlineTableName, SegmentAssignmentStrategyFactory
.getSegmentAssignmentStrategy(offlineTableConfig.getValidationConfig().getSegmentAssignmentStrategy()));
}
final SegmentAssignmentStrategy segmentAssignmentStrategy = SEGMENT_ASSIGNMENT_STRATEGY_MAP.get(offlineTableName);

// Passing a callable to this api to avoid helixHelper having which is in pinot-common having to
// depend upon pinot-controller.
Callable<List<String>> getInstancesForSegment = new Callable<List<String>>() {
@Override
public List<String> call() throws Exception {
final IdealState currentIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, offlineTableName);
final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);

if (currentInstanceSet.isEmpty()) {
final String serverTenant =
ControllerTenantNameBuilder.getOfflineTenantNameForTenant(offlineTableConfig.getTenantConfig()
.getServer());
final int replicas = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());
return segmentAssignmentStrategy.getAssignedInstances(_helixAdmin, _helixClusterName, segmentMetadata,
replicas, serverTenant);
} else {
return new ArrayList<String>(currentIdealState.getInstanceSet(segmentName));
}
}
};

HelixHelper.addSegmentToIdealState(_helixZkManager, offlineTableName, segmentName, getInstancesForSegment);
}

/**
* Returns true if the table name specified in the segment meta data has a corresponding
* realtime or offline table in the helix cluster
*
* @param segmentMetadata Meta-data for the segment
* @return
*/
private boolean matchTableName(SegmentMetadata segmentMetadata) {
if (segmentMetadata == null || segmentMetadata.getTableName() == null) {
LOGGER.error("SegmentMetadata or table name is null");
return false;
}
if ("realtime".equalsIgnoreCase(segmentMetadata.getIndexType())) {
if (getAllTableNames().contains(
TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName()))) {
return true;
}
} else {
if (getAllTableNames().contains(
TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName()))) {
return true;
}
}
LOGGER.error("Table {} is not registered", segmentMetadata.getTableName());
return false;
}

private boolean updateExistedSegment(SegmentZKMetadata segmentZKMetadata) {
final String tableName;
if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(segmentZKMetadata.getTableName());
} else {
tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentZKMetadata.getTableName());
}
final String segmentName = segmentZKMetadata.getSegmentName();

HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
PropertyKey idealStatePropertyKey = _keyBuilder.idealStates(tableName);

// Set all partitions to offline to unload them from the servers
boolean updateSuccessful;
do {
final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
for (final String instance : instanceSet) {
idealState.setPartitionState(segmentName, instance, "OFFLINE");
}
updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
} while (!updateSuccessful);

// Check that the ideal state has been written to ZK
IdealState updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
Map<String, String> instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
for (String state : instanceStateMap.values()) {
if (!"OFFLINE".equals(state)) {
LOGGER.error("Failed to write OFFLINE ideal state!");
return false;
}
}

// Wait until the partitions are offline in the external view
LOGGER.info("Wait until segment - " + segmentName + " to be OFFLINE in ExternalView");
if (!ifExternalViewChangeReflectedForState(tableName, segmentName, "OFFLINE", _externalViewOnlineToOfflineTimeout)) {
LOGGER.error("Cannot get OFFLINE state to be reflected on ExternalView changed for segment: " + segmentName);
return false;
}

// Set all partitions to online so that they load the new segment data
do {
final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
for (final String instance : instanceSet) {
idealState.setPartitionState(segmentName, instance, "ONLINE");
}
updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
} while (!updateSuccessful);

// Check that the ideal state has been written to ZK
updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
for (String state : instanceStateMap.values()) {
if (!"ONLINE".equals(state)) {
LOGGER.error("Failed to write ONLINE ideal state!");
return false;
}
}

LOGGER.info("Refresh is done for segment - " + segmentName);
return true;
}

/*
*  fetch list of segments assigned to a give table from ideal state
*/
public List<String> getAllSegmentsForResource(String tableName) {
List<String> segmentsInResource = new ArrayList<String>();
switch (TableNameBuilder.getTableTypeFromTableName(tableName)) {
case REALTIME:
for (RealtimeSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(
getPropertyStore(), tableName)) {
segmentsInResource.add(segmentZKMetadata.getSegmentName());
}

break;
case OFFLINE:
for (OfflineSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(
getPropertyStore(), tableName)) {
segmentsInResource.add(segmentZKMetadata.getSegmentName());
}
break;
default:
break;
}
return segmentsInResource;
}

public Map<String, List<String>> getInstanceToSegmentsInATableMap(String tableName) {
Map<String, List<String>> instancesToSegmentsMap = new HashMap<String, List<String>>();
IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
Set<String> segments = is.getPartitionSet();

for (String segment : segments) {
Set<String> instances = is.getInstanceSet(segment);
for (String instance : instances) {
if (instancesToSegmentsMap.containsKey(instance)) {
instancesToSegmentsMap.get(instance).add(segment);
} else {
List<String> a = new ArrayList<String>();
a.add(segment);
instancesToSegmentsMap.put(instance, a);
}
}
}

return instancesToSegmentsMap;
}

/**
* Toggle the status of segment between ONLINE (enable = true) and OFFLINE (enable = FALSE).
*
* @param tableName: Name of table to which the segment belongs.
* @param segmentName: Name of segment for which to toggle the status.
* @param enable: True for ONLINE, False for OFFLINE.
* @return
*/
public PinotResourceManagerResponse toggleSegmentState(String tableName, String segmentName, boolean enable,
long timeOutInSeconds) {
String status = (enable) ? "ONLINE" : "OFFLINE";

HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
PropertyKey idealStatePropertyKey = _keyBuilder.idealStates(tableName);

boolean updateSuccessful;
long deadline = System.currentTimeMillis() + 1000 * timeOutInSeconds;

// Set all partitions to offline to unload them from the servers
do {
final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
if (instanceSet == null || instanceSet.isEmpty()) {
return new PinotResourceManagerResponse("Segment " + segmentName + " not found.", false);
}
for (final String instance : instanceSet) {
idealState.setPartitionState(segmentName, instance, status);
}
updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
} while (!updateSuccessful && (System.currentTimeMillis() <= deadline));

// Check that the ideal state has been updated.
IdealState updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
Map<String, String> instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
for (String state : instanceStateMap.values()) {
if (!status.equals(state)) {
return new PinotResourceManagerResponse("Error: External View does not reflect ideal State, timed out (10s).",
false);
}
}

// Wait until the partitions are offline in the external view
if (!ifExternalViewChangeReflectedForState(tableName, segmentName, status, _externalViewOnlineToOfflineTimeout)) {
return new PinotResourceManagerResponse("Error: Failed to update external view, timeout", false);
}

return new PinotResourceManagerResponse(("Success: Segment " + segmentName + " is now " + status), true);
}

public PinotResourceManagerResponse dropSegment(String tableName, String segmentName) {
long timeOutInSeconds = 10;

// Put the segment in offline state first, and then delete the segment.
PinotResourceManagerResponse disableResponse = toggleSegmentState(tableName, segmentName, false, timeOutInSeconds);
if (!disableResponse.isSuccessfull()) {
return disableResponse;
}
return deleteSegment(tableName, segmentName);
}

public boolean hasRealtimeTable(String tableName) {
String actualTableName = tableName + "_REALTIME";
return getAllPinotTableNames().contains(actualTableName);
}

public boolean hasOfflineTable(String tableName) {
String actualTableName = tableName + "_OFFLINE";
return getAllPinotTableNames().contains(actualTableName);
}

public AbstractTableConfig getTableConfig(String tableName, TableType type) throws JsonParseException,
JsonMappingException, JsonProcessingException, JSONException, IOException {
String actualTableName = new TableNameBuilder(type).forTable(tableName);
AbstractTableConfig config = null;
if (type == TableType.REALTIME) {
config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
} else {
config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
}
return config;
}

public List<String> getServerInstancesForTable(String tableName, TableType type) throws JsonParseException,
JsonMappingException, JsonProcessingException, JSONException, IOException {
String actualTableName = new TableNameBuilder(type).forTable(tableName);
AbstractTableConfig config = null;
if (type == TableType.REALTIME) {
config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
} else {
config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
}
String serverTenantName =
ControllerTenantNameBuilder.getTenantName(config.getTenantConfig().getServer(), type.getServerType());
List<String> serverInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, serverTenantName);
return serverInstances;
}

public List<String> getBrokerInstancesForTable(String tableName, TableType type) throws JsonParseException,
JsonMappingException, JsonProcessingException, JSONException, IOException {
String actualTableName = new TableNameBuilder(type).forTable(tableName);
AbstractTableConfig config = null;
if (type == TableType.REALTIME) {
config = ZKMetadataProvider.getRealtimeTableConfig(getPropertyStore(), actualTableName);
} else {
config = ZKMetadataProvider.getOfflineTableConfig(getPropertyStore(), actualTableName);
}
String brokerTenantName =
ControllerTenantNameBuilder.getBrokerTenantNameForTenant(config.getTenantConfig().getBroker());
List<String> serverInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenantName);
return serverInstances;
}

public PinotResourceManagerResponse enableInstance(String instanceName) {
return toogleInstance(instanceName, true, 10);
}

public PinotResourceManagerResponse disableInstance(String instanceName) {
return toogleInstance(instanceName, false, 10);
}

/**
* Drop the instance from helix cluster. Instance will not be dropped if:
* - It is a live instance.
* - Has at least one ONLINE segment.
*
* @param instanceName: Name of the instance to be dropped.
* @return
*/
public PinotResourceManagerResponse dropInstance(String instanceName) {
if (!instanceExists(instanceName)) {
return new PinotResourceManagerResponse("Instance " + instanceName + " does not exist.", false);
}

HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
LiveInstance liveInstance = helixDataAccessor.getProperty(_keyBuilder.liveInstance(instanceName));

if (liveInstance != null) {
PropertyKey currentStatesKey =
_keyBuilder.currentStates(instanceName, liveInstance.getSessionId());
List<CurrentState> currentStates = _helixDataAccessor.getChildValues(currentStatesKey);

if (currentStates != null) {
for (CurrentState currentState : currentStates) {
for (String state : currentState.getPartitionStateMap().values()) {
if (state.equalsIgnoreCase(SegmentOnlineOfflineStateModel.ONLINE)) {
return new PinotResourceManagerResponse(("Instance " + instanceName + " has online partitions"), false);
}
}
}
} else {
return new PinotResourceManagerResponse("Cannot drop live instance " + instanceName
+ " please stop the instance first.", false);
}
}

// Disable the instance first.
toogleInstance(instanceName, false, 10);
_helixAdmin.dropInstance(_helixClusterName, getHelixInstanceConfig(instanceName));

return new PinotResourceManagerResponse("Instance " + instanceName + " dropped.", true);
}

/**
* Toggle the status of an Instance between OFFLINE and ONLINE.
* Keeps checking until ideal-state is successfully updated or times out.
*
* @param instanceName: Name of Instance for which the status needs to be toggled.
* @param toggle: 'True' for ONLINE 'False' for OFFLINE.
* @param timeOutInSeconds: Time-out for setting ideal-state.
* @return
*/
public PinotResourceManagerResponse toogleInstance(String instanceName, boolean toggle, int timeOutInSeconds) {
if (!instanceExists(instanceName)) {
return new PinotResourceManagerResponse("Instance " + instanceName + " does not exist.", false);
}

_helixAdmin.enableInstance(_helixClusterName, instanceName, toggle);
long deadline = System.currentTimeMillis() + 1000 * timeOutInSeconds;
boolean toggleSucceed = false;
String beforeToggleStates =
(toggle) ? SegmentOnlineOfflineStateModel.OFFLINE : SegmentOnlineOfflineStateModel.ONLINE;

while (System.currentTimeMillis() < deadline) {
toggleSucceed = true;
PropertyKey liveInstanceKey = _keyBuilder.liveInstance(instanceName);
LiveInstance liveInstance = _helixDataAccessor.getProperty(liveInstanceKey);
if (liveInstance == null) {
if (toggle) {
return PinotResourceManagerResponse.FAILURE_RESPONSE;
} else {
return PinotResourceManagerResponse.SUCCESS_RESPONSE;
}
}
PropertyKey instanceCurrentStatesKey =
_keyBuilder.currentStates(instanceName, liveInstance.getSessionId());
List<CurrentState> instanceCurrentStates = _helixDataAccessor.getChildValues(instanceCurrentStatesKey);
if (instanceCurrentStates == null) {
return PinotResourceManagerResponse.SUCCESS_RESPONSE;
} else {
for (CurrentState currentState : instanceCurrentStates) {
for (String state : currentState.getPartitionStateMap().values()) {
if (beforeToggleStates.equals(state)) {
toggleSucceed = false;
}
}
}
}
if (toggleSucceed) {
return (toggle) ? new PinotResourceManagerResponse("Instance " + instanceName + " enabled.", true)
: new PinotResourceManagerResponse("Instance " + instanceName + " disabled.", true);
} else {
try {
Thread.sleep(500);
} catch (InterruptedException e) {
}
}
}
return new PinotResourceManagerResponse("Instance enable/disable failed, timeout.", false);
}

/**
* Check if an Instance exists in the Helix cluster.
*
* @param instanceName: Name of instance to check.
* @return True if instance exists in the Helix cluster, False otherwise.
*/
public boolean instanceExists(String instanceName) {
HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
InstanceConfig config = helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
return (config != null);
}

public boolean isSingleTenantCluster() {
return _isSingleTenantCluster;
}

/**
* Computes the broker nodes that are untagged and free to be used.
* @return List of online untagged broker instances.
*/
public List<String> getOnlineUnTaggedBrokerInstanceList() {

final List<String> instanceList =
_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
final List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
instanceList.retainAll(liveInstances);
return instanceList;
}

/**
* Computes the server nodes that are untagged and free to be used.
* @return List of untagged online server instances.
*/
public List<String> getOnlineUnTaggedServerInstanceList() {
final List<String> instanceList =
_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
final List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
instanceList.retainAll(liveInstances);
return instanceList;
}
}
