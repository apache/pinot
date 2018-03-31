import { helper } from '@ember/component/helper';

/**
 * Template helper that computes the background color
 * for the contribution map
 * @param {Number} [value=0] Contribution change in percentage
 * @return {String}          rgba color for background
 */
export function extractDropDownValue([element, propertyKey]) {
  if (typeof element === 'object') {
    return element[propertyKey];
  }

  return element;
}

export default helper(extractDropDownValue);
