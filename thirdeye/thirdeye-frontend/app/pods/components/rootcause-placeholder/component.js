/**
 * Component to display placeholders accross the app when data is not loaded
 * @module components/rootcause-placeholder
 * @property {String}  message   - Message to display in the placeholder box
 * @property {String}  iconClass - Icon Class to be applied
 * @author yyuen
 * @example
 * {{rootcause-placeholder
 *   message="This is the placeholder message"
 *   iconClass="glyphicon glyphicon-trash"}}
 */

import Component from '@ember/component';

export default Component.extend({
  classNames: ['rootcause-placeholder'],
  message: '',
  iconClass: ''
});
