import { helper } from '@ember/component/helper';

/**
 * Template helper intended to be used with the powerselec component
 * this extracts the item's value dynamically
 * @param {Object}        element     - object found by power select
 * @param {String}        propertyKey - key property to extract
 * @return {String}         - value to be extracted
 */
export function extractDropDownValue([element, propertyKey]) {
  if (typeof element === 'object') {
    return element[propertyKey];
  }

  return element;
}

export default helper(extractDropDownValue);
