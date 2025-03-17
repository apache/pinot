export type RebalanceServerOption = {
    name: string;
    label: string;
    type: "BOOL" | "INTEGER" | "SELECT";
    description: string;
    defaultValue: string | boolean | number;
    isAdvancedConfig: boolean;
    isStatsGatheringConfig: boolean;
    hasBreakingChange: boolean;
    allowedValues?: string[];
    toolTip?: string;
}

export const rebalanceServerOptions: RebalanceServerOption[] = [
    {
        "name": "dryRun",
        "defaultValue": false,
        "label": "Dry Run",
        "type": "BOOL",
        "description": "If enabled, rebalance will not run but expected changes that will occur will be returned",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": true,
        "hasBreakingChange": false
    },
    {
        "name": "preChecks",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Pre-Checks",
        "description": "If enabled, will perform some pre-checks to ensure rebalance is safe, must enable dryRun to enable this",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": true,
        "hasBreakingChange": false
    },
    {
        "name": "reassignInstances",
        "defaultValue": true,
        "type": "BOOL",
        "label": "Reassign Instances",
        "description": "Reassigns the instances of the table prior to segment assignment if explicit assignment is enabled, no-op otherwise",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": false
    },
    {
        "name": "includeConsuming",
        "defaultValue": true,
        "type": "BOOL",
        "label": "Include Consuming",
        "description": "CONSUMING segments are rebalanced only if this is set to true, mandatory for upsert/dedup tables, ignored for OFFLINE tables",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": false
    },
    {
        "name": "minimizeDataMovement",
        "defaultValue": "ENABLE",
        "type": "SELECT",
        "allowedValues": ["ENABLE", "DISABLE", "DEFAULT"],
        "label": "Minimize Data Movement",
        "description": "Minimize data movement if set to ENABLE, disable if set to DISABLE, and use minimizeDataMovement value in TableConfig if DEFAULT. Minimize data movement only applies for explicit assignment and is a no-op otherwise",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": true,
        "toolTip": "Disabling minimizeDataMovement can cause a large amount of data movement"
    },
    {
        "name": "bootstrap",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Bootstrap",
        "description": "Regardless of minimum segment movement, reassign all segments in a round-robin fashion as if adding new segments to an empty table",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": true,
        "toolTip": "Enabling bootstrap can cause a large amount of data movement"
    },
    {
        "name": "downtime",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Downtime",
        "description": "Whether to allow downtime for rebalance or not, must be set to true if replication = 1",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": true,
        "toolTip": "Enabling can cause downtime even if replication > 1"
    },
    {
        "name": "minAvailableReplicas",
        "defaultValue": -1,
        "type": "INTEGER",
        "label": "Min Available Replicas",
        "description": "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum number of replicas allowed to be unavailable if value is negative. Should not be 0 unless for downtime=true",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": false
    },
    {
        "name": "lowDiskMode",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Low Disk Mode",
        "description": "Perform rebalance by offloading segments off servers prior to adding them. Can slow down rebalance and is recommended to enable for scenarios which are low on disk capacity",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": false
    },
    {
        "name": "bestEfforts",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Best Efforts",
        "description": "Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract cannot be achieved). This can cause downtime if IS-EV convergence fails or segments get into ERROR state",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": true,
        "toolTip": "Enabling can cause downtime even if downtime = true"
    },
    {
        "name": "externalViewStabilizationTimeoutInMs",
        "defaultValue": 3600000,
        "type": "INTEGER",
        "label": "External View Stabilization Timeout In Milliseconds",
        "description": "How long to wait for EV-IS convergence, increase this timeout for large tables (TBs in size)",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": false
    },
    {
        "name": "maxAttempts",
        "defaultValue": 3,
        "type": "INTEGER",
        "label": "Max Attempts",
        "description": "Max number of attempts to rebalance",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": false
    },
    {
        "name": "updateTargetTier",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Update Target Tier",
        "description": "Whether to update segment target tier as part of the rebalance, enable if remote tier should be rebalanced",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "hasBreakingChange": false
    }
]