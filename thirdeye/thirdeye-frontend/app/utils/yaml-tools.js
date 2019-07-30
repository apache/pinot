import yamljs from 'yamljs';
import jsyaml from 'js-yaml';

import yamljs from 'yamljs';
import jsyaml from 'js-yaml';

export const defaultDetectionYaml = `# Below is a sample template. You may refer the documentation for more examples and update the fields accordingly.
# Give a name for this anomaly detection pipeline (should be unique).
detectionName: name_of_the_detection
# Tell the alert recipients what it means if this alert is fired.
description: If this alert fires then it means so-and-so and check so-and-so for irregularities
# The metric you want to do anomaly detection on. You may type a few characters and look ahead (ctrl + space) to auto-fill.
metric: metric_name
# The dataset or UMP table name to which the metric belongs. Look ahead should auto populate this field.
dataset: dataset_name
rules:                            # Can configure multiple rules with "OR" relationship.
- detection:
    - name: detection_rule_1
      type: ALGORITHM             # Configure the detection type here. See doc for more details.
      params:                     # The parameters for this rule. Different rules have different params.
        configuration:
          bucketPeriod: P1D       # Use PT1H for hourly and PT5M for minute level (ingraph metrics) data.
          pValueThreshold: 0.05   # Higher value means more sensitive to small changes.
          mlConfig: true          # The machine learning auto config to select and maintain the configuration with the best performance.
  filter:                         # Filter out anomalies detected by rules to reduce noise.
    - name: filter_rule_1
      type: PERCENTAGE_CHANGE_FILTER
      params:
        pattern: UP_OR_DOWN       # Other patterns: "UP","DOWN".
        threshold: 0.05           # Filter out all changes less than 5% compared to baseline.
`;

export const defaultSubscriptionYaml = `# Below is a sample subscription group template. You may refer the documentation and update accordingly.
# The name of the subscription group. You may choose an existing or a provide a new subscription group name
subscriptionGroupName: test_subscription_group
# Every alert in ThirdEye is attached to an application. Please specify the registered application name here. You may request for a new application by dropping an email to ask_thirdeye
application: thirdeye-internal
# List of detection names that you want to subscribe. Copy-paste the detection name from the above anomaly detection config here.
subscribedDetections:
  - name_of_the_detection_above
# Configure how you want to be alerted. You can receive the standard ThirdEye email alert (recommended)
# or for advanced critical use-cases setup Iris alert by referring to the documentation
alertSchemes:
- type: EMAIL
recipients:
 to:
  - "me@company.com"          # Specify alert recipient email address here
  - "me@company.com"
 cc:
  - "cc_email@company.com"
fromAddress: thirdeye-dev@linkedin.com
# Enable or disable notification of alert
active: true
# The below links will appear in the email alerts. This will help alert recipients to quickly refer and act on.
referenceLinks:
  "Oncall Runbook": "http://go/oncall"
  "Thirdeye FAQs": "http://go/thirdeyefaqs"
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
      fieldKeys.forEach(key => {
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
  } catch(error) {
    try {
      // use jsyaml package to try parsing again, since yamljs doesn't parse some edge cases
      yamlAsObject = jsyaml.safeLoad(yamlString);
    } catch (error) {
      throw new Error("yaml parsing error");
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
  }
  catch(err) {
    return null;
  }
  const fieldKeys = Object.keys(fields);
  const yamlKeys = yamlAsObject ? Object.keys(yamlAsObject) : [];
  if (yamlKeys.length > 0) {
    yamlKeys.forEach(key => {
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
      value = (typeof yamlObject[keys[i]] === type) ? yamlObject[keys[i]] : null;
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
  }
  catch(err){
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
  }
  catch(err) {
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
    let nextKey = ((keyIndex + 1) < yamlKeys.length) ? keyIndex + 1 : keyIndex;
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
            if (oldYamlIndex < oldLines.length && oldLines[oldYamlIndex].includes(`${yamlKeys[nextKey]}:`) && oldLines[oldYamlIndex].charAt(0) !== '#') {
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
              if (oldYamlIndex < oldLines.length && oldLines[oldYamlIndex].includes(`${yamlKeys[nextKey]}:`) && oldLines[oldYamlIndex].charAt(0) !== '#') {
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
  if (filters){
    const filterStrings = [];
    Object.keys(filters).forEach(
      function(filterKey) {
        const filter = filters[filterKey];
        if (filter && Array.isArray(filter)) {
          filter.forEach(
            function (filterValue) {
              filterStrings.push(filterKey + '=' + filterValue);
            }
          );
        } else {
          filterStrings.push(filterKey + '=' + filter);
        }
      }
    );
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

export default {
  defaultDetectionYaml,
  defaultSubscriptionYaml,
  formatYamlFilter,
  getValueFromYaml,
  fieldsToYaml,
  postYamlProps,
  redundantParse
};
