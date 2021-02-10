/**
 * Component to render subscription group-monitored alerts table across self-serve views
 * @module components/self-serve-config-group-table
 * @property {String} title  - table title containing config group name
 * @property {Object} data  - table data for each alert
 * @example
  {{self-serve-config-group-table
    title=selectedConfigGroupSubtitle
    data=selectedGroupFunctions
  }}
 * @exports self-serve-config-group-table
 * @author smcclung
 */

import Component from '@ember/component';

/* eslint-disable ember/avoid-leaking-state-in-ember-objects */
export default Component.extend({
  title: '',
  data: {},

  /**
   * Array to define alerts table columns for selected config group
   * @type {Array}
   */
  // TODO: Move all value in to the util folder
  alertsTableColumns: [
    {
      propertyName: 'id',
      title: 'Id',
      routeName: 'manage.alert',
      className: 'te-form__table-index'
    },
    {
      propertyName: 'name',
      title: 'Alert Name',
      className: 'te-form__table-name'
    },
    {
      propertyName: 'metric',
      title: 'Alert Metric',
      className: 'te-form__table-metric'
    },
    {
      propertyName: 'owner',
      title: 'Created By'
    },
    {
      propertyName: 'status',
      title: 'Status'
    }
  ]
});
