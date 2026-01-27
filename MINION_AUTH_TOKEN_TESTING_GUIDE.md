# Minion Authentication Token Testing Guide

## Overview

Minion uses two authentication tokens:
1. **`pinot.minion.segment.fetcher.auth.token`** - For fetching segments from Controller
2. **`pinot.minion.task.auth.token`** - For task management communication with Controller

> **Note**: Legacy configurations using `segment.fetcher.auth.token` and `task.auth.token` (without `pinot.minion.` prefix) may still work for backward compatibility, but the prefixed versions are recommended for consistency with Controller and Server configurations.

---

## Configuration Files Created

### 1. Minion WITHOUT Tokens (Test Error Scenario)
**File**: `test-configs/minion-no-token.conf`
```properties
pinot.minion.port=9514
pinot.minion.helix.cluster.name=PinotCluster
pinot.minion.dataDir=/tmp/PinotMinion/data
pinot.minion.tempDir=/tmp/PinotMinion/temp

# NO TOKENS - Will cause 403 errors
# pinot.minion.segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
# pinot.minion.task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
```

### 2. Minion WITH Tokens (Working Scenario)
**File**: `test-configs/minion-with-token.conf`
```properties
pinot.minion.port=9514
pinot.minion.helix.cluster.name=PinotCluster
pinot.minion.dataDir=/tmp/PinotMinion/data
pinot.minion.tempDir=/tmp/PinotMinion/temp

# TOKENS CONFIGURED - Will work correctly
pinot.minion.segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
pinot.minion.task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
```

---

## Start Minion Commands

### Start Minion WITHOUT Tokens (To See Errors)

```bash
export PATH=/Users/akedia/Downloads/apache-maven-3.9.10/bin:$PATH

cd /Users/akedia/pinot/os/pinot/build && ./bin/pinot-admin.sh StartMinion \
  -zkAddress localhost:2181 \
  -configFileName /Users/akedia/pinot/os/pinot/test-configs/minion-no-token.conf
```

### Start Minion WITH Tokens (Working)

```bash
export PATH=/Users/akedia/Downloads/apache-maven-3.9.10/bin:$PATH

cd /Users/akedia/pinot/os/pinot/build && ./bin/pinot-admin.sh StartMinion \
  -zkAddress localhost:2181 \
  -configFileName /Users/akedia/pinot/os/pinot/test-configs/minion-with-token.conf
```

---

## When Minion Uses These Tokens

### 1. `segment.fetcher.auth.token`

**Used When**: Minion needs to download segments from Controller for processing

**Example Tasks**:
- **SegmentConversionTask**: Converting segment format
- **MergeRollupTask**: Merging segments
- **RealtimeToOfflineTask**: Converting realtime to offline segments

**Flow**:
```
1. Controller assigns task to Minion
   ↓
2. Minion needs to fetch segment from Controller
   ↓
3. Minion makes HTTP GET: http://localhost:9001/segments/...
   ↓
4. Minion adds Authorization header (pinot.minion.segment.fetcher.auth.token)
   ↓
5. Controller validates token
   ↓
6. ✅ Success (if token valid)
   ❌ 403 Forbidden (if token missing/invalid)
```

### 2. `task.auth.token`

**Used When**: Minion communicates with Controller for task management

**Operations**:
- Fetching task configurations
- Reporting task status
- Updating task progress

**Flow**:
```
1. Minion polls Controller for tasks
   ↓
2. Minion makes HTTP request to Controller API
   ↓
3. Minion adds Authorization header (pinot.minion.task.auth.token)
   ↓
4. Controller validates token
   ↓
5. ✅ Success (if token valid)
   ❌ 403 Forbidden (if token missing/invalid)
```

---

## How to Test Minion Authentication

### Prerequisites

1. **Controller running** with BasicAuth enabled
2. **Server running** with data
3. **Table with segments** available

### Test Scenario: Segment Conversion Task

#### Step 1: Create a Minion Task Configuration

Create a task config that triggers segment processing:

```bash
curl -X POST http://localhost:9001/tasks/schedule \
  -H "Content-Type: application/json" \
  -u admin:verysecret \
  -d '{
    "taskType": "SegmentGenerationAndPushTask",
    "tableName": "airlineStats_OFFLINE",
    "taskConfigs": {
      "SegmentGenerationAndPushTask": {
        "schedule": "0 */10 * * * ?"
      }
    }
  }'
```

#### Step 2: Start Minion WITHOUT Tokens

```bash
cd /Users/akedia/pinot/os/pinot/build && ./bin/pinot-admin.sh StartMinion \
  -zkAddress localhost:2181 \
  -configFileName /Users/akedia/pinot/os/pinot/test-configs/minion-no-token.conf
```

#### Step 3: Check Minion Logs for Errors

```bash
# Find Minion log file
find /Users/akedia/pinot/os/pinot -name "*Minion*.log" -type f

# Check for 403 errors
grep -i "403\|forbidden" /Users/akedia/pinot/os/pinot/pinotMinion.log | tail -20
```

**Expected Errors**:
```
ERROR [HttpSegmentFetcher] Got error status code: 403 for URI: http://localhost:9001/segments/...
ERROR [MinionTaskRunner] Failed to fetch segment - authentication failed
ERROR [TaskExecutor] Task execution failed due to authentication error
```

#### Step 4: Restart Minion WITH Tokens

```bash
# Stop Minion
pkill -f "StartMinion"

# Start with tokens
cd /Users/akedia/pinot/os/pinot/build && ./bin/pinot-admin.sh StartMinion \
  -zkAddress localhost:2181 \
  -configFileName /Users/akedia/pinot/os/pinot/test-configs/minion-with-token.conf
```

#### Step 5: Verify Success

```bash
# Check Minion logs for successful task execution
grep -i "task.*complete\|segment.*fetched" /Users/akedia/pinot/os/pinot/pinotMinion.log | tail -20
```

**Expected Success Messages**:
```
INFO [HttpSegmentFetcher] Successfully fetched segment from Controller
INFO [MinionTaskRunner] Task execution completed successfully
INFO [TaskExecutor] Segment processing completed
```

---

## Code Locations

### Where Tokens Are Used

#### 1. Segment Fetcher Token
**File**: `pinot-common/src/main/java/org/apache/pinot/common/utils/fetcher/HttpSegmentFetcher.java`

```java
String authToken = _authProvider.getAuthToken();
if (authToken != null) {
  httpUriRequest.setHeader(HttpHeaders.AUTHORIZATION, authToken);
}
```

#### 2. Task Auth Token
**File**: `pinot-minion/src/main/java/org/apache/pinot/minion/BaseMinionStarter.java`

```java
String taskAuthToken = _minionConf.getProperty("pinot.minion.task.auth.token");
if (taskAuthToken != null) {
  _httpClient.setAuthToken(taskAuthToken);
}
```

---

## Validation Checklist

### ✅ Setup:
- [ ] Controller running with BasicAuth (admin/verysecret)
- [ ] Server running with data
- [ ] Table with segments available
- [ ] Minion config files created

### ✅ Test WITHOUT Tokens:
- [ ] Start Minion with minion-no-token.conf
- [ ] Trigger a Minion task
- [ ] Check logs for 403 Forbidden errors
- [ ] Verify task fails due to authentication

### ✅ Test WITH Tokens:
- [ ] Stop Minion
- [ ] Start Minion with minion-with-token.conf
- [ ] Trigger a Minion task
- [ ] Check logs for successful execution
- [ ] Verify task completes successfully

---

## Quick Test Commands

### Complete Test Flow:

```bash
# 1. Start Minion WITHOUT tokens
cd /Users/akedia/pinot/os/pinot/build && ./bin/pinot-admin.sh StartMinion \
  -zkAddress localhost:2181 \
  -configFileName /Users/akedia/pinot/os/pinot/test-configs/minion-no-token.conf &

# 2. Wait a few seconds, then check logs
sleep 5
grep -i "403\|forbidden" /Users/akedia/pinot/os/pinot/pinotMinion.log

# 3. Stop and restart with tokens
pkill -f "StartMinion"
sleep 2

cd /Users/akedia/pinot/os/pinot/build && ./bin/pinot-admin.sh StartMinion \
  -zkAddress localhost:2181 \
  -configFileName /Users/akedia/pinot/os/pinot/test-configs/minion-with-token.conf &

# 4. Check for success
sleep 5
grep -i "success\|complete" /Users/akedia/pinot/os/pinot/pinotMinion.log
```

---

## Summary

### Two Tokens for Minion:

| Token | Purpose | Used When |
|-------|---------|-----------|
| `pinot.minion.segment.fetcher.auth.token` | Fetch segments from Controller | Minion downloads segments for processing |
| `pinot.minion.task.auth.token` | Task management API calls | Minion communicates with Controller for tasks |

### Error When Missing:
- **403 Forbidden** when Minion tries to fetch segments
- **403 Forbidden** when Minion tries to communicate with Controller
- **Task execution fails**

### Token Format:
```
Basic YWRtaW46dmVyeXNlY3JldA==
```
This is base64 encoding of `admin:verysecret`:
```bash
echo -n "admin:verysecret" | base64
```

---

## All Configuration Files Ready:

1. ✅ `test-configs/minion-no-token.conf` - For testing errors
2. ✅ `test-configs/minion-with-token.conf` - For working scenario
3. ✅ Start commands documented
4. ✅ Validation steps provided

**Ready to test Minion authentication!**
