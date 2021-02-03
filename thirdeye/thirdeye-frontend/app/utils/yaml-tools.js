import yamljs from 'yamljs';
import jsyaml from 'js-yaml';

export const defaultDetectionYaml = `detectionName: 'give_a_unique_name_to_this_alert'
description: 'If this alert fires then it means ...'

# Tip: Type a few characters and look ahead (ctrl + space) to auto-fill.
metric: metric_to_monitor
dataset: dataset_to_which_this_metric_belongs

# Configure multiple rules with "OR" relationship.
rules:
- detection:
  - name: detection_rule_1
    type: ALGORITHM             # Configure the detection type here. See doc for more details.
    params:                     # The parameters for this rule. Different rules have different params.
      configuration:
        bucketPeriod: P1D       # Use P1D for daily; PT1H for hourly; PT5M for minutely data.
        pValueThreshold: 0.05   # Higher value means more sensitive to small changes.
        mlConfig: true          # Automatically maintain configuration with the best performance.
  filter:                       # Filter out anomalies detected by rules to reduce noise.
  - name: filter_rule_1
    type: PERCENTAGE_CHANGE_FILTER
    params:
      pattern: UP_OR_DOWN       # Other patterns: "UP","DOWN".
      threshold: 0.05           # Filter out all changes less than 5% compared to baseline.
  quality:                      # Configure the data quality rules
  - name: data_sla_rule_1
    type: DATA_SLA              # Alert if data is missing.
    params:
      sla: 3_DAYS               # Data is missing for 3 days since last availability
`;

export const defaultSubscriptionYaml = `subscriptionGroupName: 'give_a_unique_name_to_this_group'

# Specify your registered application name here. Contact admin/team to register a new one.
application: thirdeye-internal

# List all alerts (detectionName) you want to subscribe to.
subscribedDetections:
- 'list_the_detection_you_want_to_subscribe'

# Setup Email, Jira or other notification channels
alertSchemes:
- type: EMAIL
  params:
    recipients:
      to:
      - me@company.com
      - you@company.com
      cc:
      - manager@company.com

# Make these links appear in the alert notifications
# referenceLinks:
#   "Alert Runbook": "link_to_your_product_runbook"
`;

/**
 * Recursive helper function for merging two javascript objects
 * @method mergeContents
 * @param {Object} OR {Array} fieldObject taken from field
 * @param {Object} OR {Array} yamlObject piece of parsed yaml
 */
function mergeContents(fieldObject, yamlObject) {
  let output = yamlObject; // default to just return original value
  if (Array.isArray(fieldObject) && Array.isArray(yamlObject)) {
    // for arrays, we keep old and add new
    output = [...new Set([...fieldObject, ...yamlObject])];
  } else if (typeof fieldObject === 'string' && typeof yamlObject === 'string') {
    // for strings, we replace old with new
    output = fieldObject;
  } else if (typeof fieldObject === 'object' && typeof yamlObject === 'object') {
    // if both are objects, match keys and recurse
    output = {};
    const fieldKeys = Object.keys(fieldObject);
    const yamlKeys = yamlObject ? Object.keys(yamlObject) : [];
    if (yamlKeys.length > 0) {
      fieldKeys.forEach((key) => {
        if (yamlKeys.includes(key)) {
          // merge yaml contents and passed in contents
          output[key] = mergeContents(fieldObject[key], yamlObject[key]);
        }
      });
    }
  }
  return output;
}

/**
 * helper for parsing yaml with backup
 * @method redundantParse
 * @param {String} yamlString (the yaml string to parse)
 */
export function redundantParse(yamlString) {
  let yamlAsObject = {};
  try {
    yamlAsObject = yamljs.parse(yamlString);
  } catch (error) {
    try {
      // use jsyaml package to try parsing again, since yamljs doesn't parse some edge cases
      yamlAsObject = jsyaml.safeLoad(yamlString);
    } catch (error) {
      throw new Error('yaml parsing error');
    }
  }
  return yamlAsObject;
}

/**
 * This function will update yaml according to form input arrays and strings from the user
 * @method fieldsToYaml
 * @param {Object} fields (assumes keys are same as target fields in yaml)
 * @param {String} yamlString (the yaml string to be updated)
 * @return {String} - updatedYaml
 */
export function fieldsToYaml(fields, yamlString) {
  // parse yamlString to JSON
  let yamlAsObject = {};
  try {
    yamlAsObject = redundantParse(yamlString);
  } catch (err) {
    return null;
  }
  const fieldKeys = Object.keys(fields);
  const yamlKeys = yamlAsObject ? Object.keys(yamlAsObject) : [];
  if (yamlKeys.length > 0) {
    yamlKeys.forEach((key) => {
      if (fieldKeys.includes(key) && fields[key]) {
        // merge yaml contents and passed in contents, then map stringified version
        yamlAsObject[key] = mergeContents(fields[key], yamlAsObject[key]);
      }
    });
  }
  // insert comments back into yaml.
  yamlString = addBackComments(yamlAsObject, yamlString);
  return yamlString;
}

/**
 * Recursive helper function for getting values from yaml
 * @method traverseYaml
 * @param {String} field (field to look for in Yaml)
 * @param {Object} yamlObject (the yaml object to search in)
 * @param {String} type (the type of value expected)
 */
function traverseYaml(field, yamlObject, type) {
  const keys = Object.keys(yamlObject);
  let value = null;
  for (let i = 0; i < keys.length; i++) {
    if (keys[i] === field) {
      value = typeof yamlObject[keys[i]] === type ? yamlObject[keys[i]] : null;
    } else if (yamlObject[keys[i]] && typeof yamlObject[keys[i]] === 'object') {
      value = traverseYaml(field, yamlObject[keys[i]]);
    }
    if (value) {
      break;
    }
  }
  return value;
}

/**
 * This function will return the value of a given field in a yaml string if it exists
 * @method getValueFromYaml
 * @param {String} field (field to look for in Yaml)
 * @param {String} yamlString (the yaml string to search in)
 * @param {String} type (the type of value expected)
 */
export function getValueFromYaml(field, yamlString, type) {
  // parse yamlString to JSON
  let yamlAsObject = {};
  try {
    yamlAsObject = redundantParse(yamlString);
  } catch (err) {
    return null;
  }
  return yamlAsObject ? traverseYaml(field, yamlAsObject, type) : null;
}

/**
 * This function will preserve comments and blank lines and do a "best effort" to keep in the correct place
 * @method addBackComments
 * @param {Object} fields (assumes keys are same as target fields in yaml)
 * @param {String} yamlString (the yaml string to be updated)
 * @return {String} - updatedYaml
 */
export function addBackComments(yamlAsObject, yamlString) {
  // stringify new yaml
  let newYaml;
  let yamlKeys = Object.keys(yamlAsObject); // this should not change since we only update fields
  try {
    newYaml = yamljs.stringify(yamlAsObject, 20, 1);
  } catch (err) {
    return null;
  }
  // split yamls into array of strings, line by line
  let oldLines = yamlString.split('\n');
  let newLines = newYaml.split('\n');
  const mergedLines = [];
  // iterate through all keys of yaml, so we can replace all lines allocated to a key
  let newYamlIndex = 0;
  let oldYamlIndex = 0; // this will point at the lines of old yaml
  let keyIndex = 0; // this is for iterating through the keys
  // iterate through split new yaml
  while (newYamlIndex < newLines.length && oldYamlIndex < oldLines.length && keyIndex < yamlKeys.length) {
    let nextKey = keyIndex + 1 < yamlKeys.length ? keyIndex + 1 : keyIndex;
    if (newLines[newYamlIndex].includes(`${yamlKeys[keyIndex]}:`)) {
      let foundStart = false;
      // loop through old yaml string and add comments and/or blank lines as needed
      while (!foundStart && newYamlIndex < newLines.length && oldYamlIndex < oldLines.length) {
        if (oldLines[oldYamlIndex].includes(`${yamlKeys[keyIndex]}:`) && oldLines[oldYamlIndex].charAt(0) !== '#') {
          // start of same key
          foundStart = true;
          let foundEndOld = false;
          let foundEndNew = false;
          while (!foundEndOld && !foundEndNew && oldYamlIndex < oldLines.length && newYamlIndex < newLines.length) {
            if (oldLines[oldYamlIndex].charAt(0) !== '#') {
              const thisLine = oldLines[oldYamlIndex].split('#');
              if (thisLine.length > 1) {
                // there are comments, keep them
                mergedLines.push(newLines[newYamlIndex] + ' #' + thisLine.slice(1).join('#'));
                newYamlIndex++;
                oldYamlIndex++;
              } else {
                // replace whole line
                mergedLines.push(newLines[newYamlIndex]);
                newYamlIndex++;
                oldYamlIndex++;
              }
            } else {
              // whole line is comment
              mergedLines.push(oldLines[oldYamlIndex]);
              oldYamlIndex++;
            }
            if (
              oldYamlIndex < oldLines.length &&
              oldLines[oldYamlIndex].includes(`${yamlKeys[nextKey]}:`) &&
              oldLines[oldYamlIndex].charAt(0) !== '#'
            ) {
              foundEndOld = true;
            }
            if (newYamlIndex < newLines.length && newLines[newYamlIndex].includes(`${yamlKeys[nextKey]}:`)) {
              foundEndNew = true;
            }
          }
          // take care of remaining lines
          if (!foundEndOld) {
            while (!foundEndOld && oldYamlIndex < oldLines.length) {
              if (oldLines[oldYamlIndex].charAt(0) === '#') {
                // additional comment line, add it
                mergedLines.push(oldLines[oldYamlIndex]);
                oldYamlIndex++;
              } else {
                const thisLine = oldLines[oldYamlIndex].split('#');
                if (thisLine.length > 1) {
                  mergedLines.push(['', thisLine.slice(1).join('#')].join('#'));
                }
                oldYamlIndex++;
              }
              if (
                oldYamlIndex < oldLines.length &&
                oldLines[oldYamlIndex].includes(`${yamlKeys[nextKey]}:`) &&
                oldLines[oldYamlIndex].charAt(0) !== '#'
              ) {
                foundEndOld = true;
              }
            }
          }
          if (!foundEndNew) {
            while (!foundEndNew && newYamlIndex < newLines.length) {
              // for new, we just add the lines until the next key or end of array
              mergedLines.push(newLines[newYamlIndex]);
              newYamlIndex++;
              if (newYamlIndex < newLines.length && newLines[newYamlIndex].includes(`${yamlKeys[nextKey]}:`)) {
                foundEndNew = true;
              }
            }
          }
        } else {
          // blank line or comment - add to mergedLines
          mergedLines.push(oldLines[oldYamlIndex]);
          oldYamlIndex++;
        }
      }
      keyIndex++;
    } else {
      // totally new line, insert to merged result
      mergedLines.push(newLines[newYamlIndex]);
      newYamlIndex++;
    }
  }
  yamlString = mergedLines.join('\n');
  return yamlString;
}

/**
 * The yaml filters formatter. Convert filters in the yaml file in to a legacy filters string
 * For example, filters = {
 *   "country": ["us", "cn"],
 *   "browser": ["chrome"]
 * }
 * will be convert into "country=us;country=cn;browser=chrome"
 *
 * @method formatYamlFilter
 * @param {Map} filters multimap of filters
 * @return {String} - formatted filters string
 */
export function formatYamlFilter(filters) {
  if (filters) {
    const filterStrings = [];
    Object.keys(filters).forEach(function (filterKey) {
      const filter = filters[filterKey];
      if (filter && Array.isArray(filter)) {
        filter.forEach(function (filterValue) {
          filterStrings.push(filterKey + '=' + filterValue);
        });
      } else {
        filterStrings.push(filterKey + '=' + filter);
      }
    });
    return filterStrings.join(';');
  }
  return '';
}

/**
 * Preps post object for Yaml payload
 * @param {string} text to post
 * @returns {Object}
 */
export function postYamlProps(postData) {
  return {
    method: 'post',
    body: postData,
    headers: { 'content-type': 'text/plain' }
  };
}

export function enrichAlertResponseObject(alerts) {
  for (let yamlAlert of alerts) {
    let dimensions = '';
    let dimensionsArray = yamlAlert.dimensionExploration ? yamlAlert.dimensionExploration.dimensions : null;
    if (Array.isArray(dimensionsArray)) {
      dimensionsArray.forEach((dim) => {
        dimensions = dimensions + `${dim}, `;
      });
      dimensions = dimensions.substring(0, dimensions.length - 2);
    }
    Object.assign(yamlAlert, {
      functionName: yamlAlert.name,
      collection: yamlAlert.datasetNames.toString(),
      granularity: yamlAlert.monitoringGranularity.toString(),
      type: _detectionType(yamlAlert),
      exploreDimensions: dimensions,
      filters: formatYamlFilter(yamlAlert.filters),
      isNewPipeline: true,
      group: Array.isArray(yamlAlert.subscriptionGroup) ? yamlAlert.subscriptionGroup.join(', ') : null
    });
  }

  return alerts;
}

/**
 * Grab detection type if available, else return yamlAlert.pipelineType
 */
function _detectionType(yamlAlert) {
  if (yamlAlert.rules && Array.isArray(yamlAlert.rules) && yamlAlert.rules.length > 0) {
    if (
      yamlAlert.rules[0].detection &&
      Array.isArray(yamlAlert.rules[0].detection) &&
      yamlAlert.rules[0].detection.length > 0
    ) {
      return yamlAlert.rules[0].detection[0].type;
    }
  }
  return yamlAlert.pipelineType;
}

// Maps filter name to alert property for filtering
export const filterToPropertyMap = {
  application: 'application',
  subscription: 'group',
  owner: 'createdBy',
  type: 'type',
  metric: 'metric',
  dataset: 'collection',
  granularity: 'granularity'
};

// Maps filter name to alerts API params for filtering
export const filterToParamsMap = {
  application: 'application',
  subscription: 'subscriptionGroup',
  owner: 'createdBy',
  type: 'ruleType',
  metric: 'metric',
  dataset: 'dataset',
  granularity: 'granularity',
  status: 'active',
  names: 'names'
};

export function populateFiltersLocal(originalAlerts, rules) {
  // This filter category is "secondary". To add more, add an entry here and edit the controller's "filterToPropertyMap"
  const filterBlocksLocal = [
    {
      name: 'status',
      title: 'Status',
      type: 'checkbox',
      selected: ['Active', 'Inactive'],
      filterKeys: ['Active', 'Inactive']
    },
    {
      name: 'application',
      title: 'Applications',
      type: 'search',
      matchWidth: true,
      hasNullOption: true, // allow searches for 'none'
      filterKeys: []
    },
    {
      name: 'subscription',
      title: 'Subscription Groups',
      hasNullOption: true, // allow searches for 'none'
      type: 'search',
      filterKeys: []
    },
    {
      name: 'owner',
      title: 'Owners',
      type: 'search',
      matchWidth: true,
      filterKeys: []
    },
    {
      name: 'type',
      title: 'Detection Type',
      type: 'select',
      filterKeys: rules
    },
    {
      name: 'metric',
      title: 'Metrics',
      type: 'search',
      filterKeys: []
    },
    {
      name: 'dataset',
      title: 'Datasets',
      type: 'search',
      filterKeys: []
    }
  ];
  return filterBlocksLocal;
}

export default {
  defaultDetectionYaml,
  defaultSubscriptionYaml,
  enrichAlertResponseObject,
  filterToParamsMap,
  filterToPropertyMap,
  formatYamlFilter,
  getValueFromYaml,
  fieldsToYaml,
  populateFiltersLocal,
  postYamlProps,
  redundantParse
};
