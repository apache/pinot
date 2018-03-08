import { helper } from '@ember/component/helper';

/**
 * Determines whether the text should be negative or positive colored, depending on whether the input is positive or
 * negative
 * @param {Float} float - numerical input to determine color
 * @return {String} - 'up' or 'down', depending on the sign of the input
 */
export function calculateDirection(float) {
  const direction = float < 0 ? 'down' : 'up';
  return direction;
}

export default helper(calculateDirection);
