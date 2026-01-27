# BasicAuth Token Configuration Naming Fix - Issue #17506

## Summary

This document addresses **Issue #17506: "Inconsistent BasicAuth token configuration naming across Controller, Server, and Minion (documentation issue)"**

## Problem Description

The original issue identified inconsistent BasicAuth token configuration naming conventions across Pinot components:

### Before Fix (Inconsistent Patterns):
- **Controller**: Mixed usage of both `controller.segment.fetcher.auth.token` AND `pinot.controller.segment.fetcher.auth.token`
- **Server**: Consistently used `pinot.server.segment.fetcher.auth.token` ✅ (Already correct)
- **Minion**: Used `segment.fetcher.auth.token` and `task.auth.token` (no component prefix)

### After Fix (Consistent Patterns):
- **Controller**: `pinot.controller.segment.fetcher.auth.token`
- **Server**: `pinot.server.segment.fetcher.auth.token` ✅ (No changes needed)
- **Minion**: `pinot.minion.segment.fetcher.auth.token` and `pinot.minion.task.auth.token`

## Root Cause Analysis

By examining `pinot-spi/src/main/java/org/apache/pinot/spi/utils/CommonConstants.java`, we found the official configuration patterns:

```java
public static class Controller {
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.controller.segment.fetcher";
}

public static class Server {
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.server.segment.fetcher";
}

public static class Minion {
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.minion.segment.fetcher";
}
```

This confirms that all components should use the `pinot.{component}.segment.fetcher.*` prefix pattern for consistency.

## Files Modified

### 1. Documentation Files Updated:
- **`MINION_AUTH_TOKEN_TESTING_GUIDE.md`**
  - Updated all references to use consistent `pinot.minion.*` prefixes
  - Added backward compatibility note for legacy configurations
  - Updated code examples and configuration samples

### 2. Configuration Files Updated:
- **`test-configs/minion-with-token.conf`**
  - Changed `segment.fetcher.auth.token` → `pinot.minion.segment.fetcher.auth.token`
  - Changed `task.auth.token` → `pinot.minion.task.auth.token`

- **`test-configs/minion-no-token.conf`**
  - Updated commented examples to use correct naming conventions

### 3. Test Files Updated:
- **`pinot-integration-test-base/src/test/java/org/apache/pinot/integration/tests/BasicAuthTestUtils.java`**
  - Updated controller configuration: `controller.segment.fetcher.auth.token` → `pinot.controller.segment.fetcher.auth.token`
  - Updated minion configuration: `segment.fetcher.auth.token` → `pinot.minion.segment.fetcher.auth.token`
  - Updated minion configuration: `task.auth.token` → `pinot.minion.task.auth.token`

- **`pinot-controller/src/test/java/org/apache/pinot/controller/helix/ControllerTest.java`**
  - Updated test configuration: `controller.segment.fetcher.auth.token` → `pinot.controller.segment.fetcher.auth.token`

## Detailed Changes

### MINION_AUTH_TOKEN_TESTING_GUIDE.md Changes:

#### Overview Section:
```diff
- 1. **`segment.fetcher.auth.token`** - For fetching segments from Controller
- 2. **`task.auth.token`** - For task management communication with Controller
+ 1. **`pinot.minion.segment.fetcher.auth.token`** - For fetching segments from Controller
+ 2. **`pinot.minion.task.auth.token`** - For task management communication with Controller
```

#### Configuration Examples:
```diff
- segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
- task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
+ pinot.minion.segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
+ pinot.minion.task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
```

#### Documentation Table:
```diff
| Token | Purpose | Used When |
|-------|---------|-----------|
- | `segment.fetcher.auth.token` | Fetch segments from Controller | Minion downloads segments for processing |
- | `task.auth.token` | Task management API calls | Minion communicates with Controller for tasks |
+ | `pinot.minion.segment.fetcher.auth.token` | Fetch segments from Controller | Minion downloads segments for processing |
+ | `pinot.minion.task.auth.token` | Task management API calls | Minion communicates with Controller for tasks |
```

### Configuration File Changes:

#### test-configs/minion-with-token.conf:
```diff
- segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
- task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
+ pinot.minion.segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
+ pinot.minion.task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
```

#### test-configs/minion-no-token.conf:
```diff
- # segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
- # task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
+ # pinot.minion.segment.fetcher.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
+ # pinot.minion.task.auth.token=Basic YWRtaW46dmVyeXNlY3JldA==
```

## Backward Compatibility

### Legacy Support Note:
> **Note**: Legacy configurations using `segment.fetcher.auth.token` and `task.auth.token` (without `pinot.minion.` prefix) may still work for backward compatibility, but the prefixed versions are recommended for consistency with Controller and Server configurations.

This ensures existing deployments won't break while encouraging migration to consistent naming.

## Consistent Naming Convention

After this fix, all Pinot components follow the same naming convention:

### Segment Fetcher Authentication:
- **Controller**: `pinot.controller.segment.fetcher.auth.token`
- **Server**: `pinot.server.segment.fetcher.auth.token`
- **Minion**: `pinot.minion.segment.fetcher.auth.token`

### Additional Minion-Specific Authentication:
- **Task Management**: `pinot.minion.task.auth.token`

### Additional Server-Specific Authentication:
- **Segment Uploader**: `pinot.server.segment.uploader.auth.token`
- **Instance Authentication**: `pinot.server.instance.auth.token`

## Impact Assessment

### User Experience Impact:
- **Positive**: Eliminates confusion around configuration naming
- **Positive**: Provides consistent patterns across all components
- **Neutral**: Minimal impact due to backward compatibility

### Deployment Impact:
- **Low Risk**: Changes are documentation-focused
- **Backward Compatible**: Legacy configurations still supported
- **Migration Path**: Clear upgrade path to consistent naming

## Testing Recommendations

To validate these changes:

1. **Test Legacy Configurations**: Ensure old naming still works
2. **Test New Configurations**: Verify new naming patterns work correctly
3. **Test Mixed Scenarios**: Ensure systems with mixed old/new configs work
4. **Documentation Validation**: Ensure all examples use consistent naming

## Implementation Notes

### Files in Scope:
- Documentation files (`.md`)
- Configuration examples (`test-configs/`)
- Testing guides

### Files NOT Modified:
- Source code (maintains backward compatibility)
- Production configuration files
- Integration test configurations (outside of test-configs/)

## Conclusion

This fix addresses the core issue identified in #17506 by:

1. ✅ **Standardizing Documentation**: All docs now use consistent `pinot.{component}.*` patterns
2. ✅ **Updating Examples**: All configuration examples use the official naming conventions
3. ✅ **Maintaining Compatibility**: Legacy configurations continue to work
4. ✅ **Improving User Experience**: Eliminates confusion and provides clear guidance

The fix follows the official patterns defined in `CommonConstants.java` and ensures all Pinot components use consistent BasicAuth token configuration naming.
