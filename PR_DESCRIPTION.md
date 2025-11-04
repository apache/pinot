# Fix Minion Task Metrics Collection Gaps

## Problem Statement

The Minion subtask metrics collection had two scenarios where metrics were not being reported accurately:

1. **Previously in-progress tasks that completed between collection cycles**: Tasks that were in-progress in one collection run but completed before the next run were not included in metrics, causing their final states (especially errors) to be missed.

2. **Short-lived tasks that started and completed between collection cycles**: Tasks that started and completed entirely between two 15-minute collection cycles were never captured in metrics.

## Solution

### 1. Track Previously In-Progress Tasks
- Added `_previousInProgressTasks` map to maintain state of tasks that were in-progress in the previous execution cycle
- Compare previous and current in-progress tasks to detect completed tasks
- Include completed tasks in current cycle's metrics collection

### 2. Detect Short-Lived Tasks
- Added `getTasksInProgressAndRecent()` method in `PinotHelixTaskResourceManager` that combines in-progress tasks and recent tasks in a single Helix call
- Uses `WorkflowContext.getJobStartTimes()` to efficiently find tasks that started after a timestamp
- Avoids duplicate `getWorkflowConfig`/`getWorkflowContext` calls by combining both queries
- Include short-lived tasks in metrics collection

### 3. Dynamic Timestamp Initialization
- Replaced static `_initializationTimestamp` (set at object construction) with dynamic calculation using `currentTime - frequencyMs` on first execution
- Ensures accurate timestamps when controller becomes leader for the first time or after leader changes
- Each task type maintains its own timestamp for independent lifecycle tracking

## Key Changes

### Files Modified

1. **TaskMetricsEmitter.java**
   - Added `_previousInProgressTasks` map to track tasks across cycles
   - Changed from global `_previousExecutionTimestamp` to per-task-type `_previousExecutionTimestamps` map
   - Added dynamic timestamp initialization using `_taskMetricsEmitterFrequencyMs`
   - Implemented logic to detect and include completed tasks
   - Implemented logic to detect and include short-lived tasks
   - Added cleanup for removed task types

2. **PinotHelixTaskResourceManager.java**
   - Added `getTasksInProgressAndRecent(taskType, afterTimestampMs)` method that combines in-progress and recent tasks
   - Overloaded `getTasksInProgress(taskType)` to call `getTasksInProgressAndRecent(taskType, 0)`
   - Uses `WorkflowContext.getJobStartTimes()` for efficient batch retrieval
   - Returns tasks that are in-progress or started after the specified timestamp

3. **TaskMetricsEmitterTest.java**
   - Added comprehensive test for previously in-progress tasks that completed between runs
   - Added comprehensive test for short-lived tasks that started and completed between runs
   - Updated all comments to use contextual naming instead of "gap" terminology
   - Updated variable names for clarity (`taskCompletedBetweenRuns`, `taskShortLived`)

## Testing

- ✅ All 11 tests pass (including 2 new tests for the gaps)
- ✅ No checkstyle violations
- ✅ Comprehensive test coverage for both scenarios

## Benefits

1. **Complete Metrics**: No metrics are missed for tasks that complete quickly or between cycles
2. **Accurate Error Reporting**: Errors in fast-failing tasks are now captured
3. **Leader Change Resilience**: Dynamic timestamp initialization handles controller leadership changes correctly
4. **Per-Task-Type Independence**: Each task type maintains its own tracking, preventing cross-contamination

## Backward Compatibility

✅ Fully backward compatible - existing functionality is preserved, only extended to cover the missing scenarios.

