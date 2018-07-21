/**
 * Component for selecting and managing subscription groups in a modal
 *
 * Supports
 * - search groups via application and name,
 * - search anomaly functions from db
 * - group creation, copying and editing
 * - transactional cancel or save
 *
 * @module components/modals/manage-groups-modal
 * @property {Function} onSubmit          - save the events
 * @property {Function} onCancel          - toggle off the modal
 * @example
 {{modals/manage-groups-modal
   showManageGroupsModal
 }}
 * @exports manage-groups-modal
 * @author apucher
 */

import Component from '@ember/component';
import { computed, get, set, getProperties, setProperties } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Component.extend({

  /**
   * Custom classes to be applied
   */
  classes: Object.freeze({
    theadCell: "te-modal__table-header"
  }),

  /**
   * display flag for modal
   * @type {boolean}
   */
  showManageGroupsModal: false,

  /**
   * Action for save/confirm. Passes selected group as argument
   */
  onSave: null,

  /**
   * Action for cancellation.
   */
  onExit: null,

  didReceiveAttrs() {
    this._super(...arguments);

    // TODO use ember data API for applications
    fetch('/thirdeye/entity/APPLICATION')
      .then(checkStatus)
      .then(res => this._makeApplicationOptions(res))
      .then(options => set(this, 'applicationOptions', options));

    // TODO use ember data API for alert configs
    // TODO support incremental backend search
    fetch('/thirdeye/entity/ALERT_CONFIG')
      .then(checkStatus)
      .then(res => this._makeGroupOptions(res))
      .then(options => set(this, 'groupOptionsRaw', options));

    // TODO use ember data API for anomaly functions
    // TODO support incremental backend search
    fetch('/thirdeye/entity/ANOMALY_FUNCTION')
      .then(checkStatus)
      .then(res => this._makeFunctionOptions(res))
      .then(options => set(this, 'functionOptions', options));
  },

  /**
   * Non-editable flag for components dependent on application selection
   * @type {boolean}
   */
  cannotSelect: computed('application', function () {
    return _.isEmpty(get(this, 'application'));
  }),

  /**
   * Non-editable flag for components dependent on group selection
   * @type {boolean}
   */
  cannotEdit: computed('group', function () {
    return _.isEmpty(get(this, 'group'));
  }),

  /**
   * Modal message for input validation and errors
   * @type {string}
   */
  message: null,

  /**
   * Temp storage for group recipients from text input
   * @type {String}
   */
  groupRecipients: null,

  /**
   * Group options available in database
   * @type {Array}
   */
  groupOptionsRaw: [],

  /**
   * Group options available for selection (and search)
   * @type {Array}
   */
  groupOptions: computed('application', 'groupOptionsRaw', function () {
    const { application, groupOptionsRaw } = getProperties(this, 'application', 'groupOptionsRaw');
    const options = groupOptionsRaw.filter(opt => opt.application === null || opt.application === application);
    options[0].application = application; // set application for "new" template
    return options;
  }),

  /**
   * Currently selected group
   * @type {object}
   */
  group: null,

  /**
   * Currently selected application
   * @type {object}
   */
  application: null,

  /**
   * Application options available for selection (and search)
   * @type {Array}
   */
  applicationOptions: [],

  /**
   * Currently selected application option
   * @type {object}
   */
  applicationOptionSelected: computed('application', {
    get () {
      const { applicationOptions, application } = getProperties(this, 'applicationOptions', 'application');
      if (_.isEmpty(application)) { return; }
      return applicationOptions.find(opt => opt.application === application);
    },
    set () {
      // left blank to prevent property override
    }
  }),

  /**
   * Cron options available for selection
   * @type {Array}
   */
  cronOptions: [
    { name: 'immediately', cron: '0 0/5 * * * ? *' },
    { name: 'every 15 min', cron: '0 0/15 * * * ? *' },
    { name: 'every hour', cron: '0 0 * * * ? *' },
    { name: 'every 3 hours', cron: '0 0 0/3 * * ? *' },
    { name: 'every 6 hours', cron: '0 0 0/6 * * ? *' },
    { name: 'daily', cron: '0 0 0 * * ? *' },
    { name: 'weekly', cron: '0 0 0 * * SUN *' }
  ],

  /**
   * Currently selected cron option
   * @type {object}
   */
  cronOptionSelected: computed('group.cronExpression', {
    get () {
      const { cronOptions, group } = getProperties(this, 'cronOptions', 'group');
      if (_.isEmpty(group)) { return; }
      return cronOptions.find(opt => opt.cron === group.cronExpression);
    },
    set () {
      // left blank to prevent property override
    }
  }),

  /**
   * Function options available for selection (and search)
   * @type {Array}
   */
  functionOptions: [],

  /**
   * Cached functionIds parsed from currently selected group
   * @type {Set}
   */
  functionOptionsSelectedIds: computed('group.emailConfig.functionIds', function () {
    const functionIds = get(this, 'group.emailConfig.functionIds');
    if (_.isEmpty(functionIds)) { return []; }
    return new Set(functionIds);
  }),

  /**
   * Currently selected function ids
   * @type {Array}
   */
  functionOptionsSelected: computed('functionOptionsSelectedIds', {
    get () {
      const { functionOptions, functionOptionsSelectedIds } = getProperties(this, 'functionOptions', 'functionOptionsSelectedIds');
      if (_.isEmpty(functionOptionsSelectedIds)) { return []; }
      return functionOptions.filter(opt => functionOptionsSelectedIds.has(opt.id));
    },
    set () {
      // left blank to prevent property override
    }
  }),

  /**
   * Parses application entities into power-select options
   *
   * @param res {Array} APPLICATION entities
   * @returns {Array} application options
   * @private
   */
  _makeApplicationOptions (res) {
    return res.map(r => Object.assign({}, { name: r.application, application: r.application })).sortBy('name');
  },

  /**
   * Parses alert group entities into power-select options
   *
   * @param res {Array} ALERT_CONFIG entities
   * @returns {Array} groups
   * @private
   */
  _makeGroupOptions (res) {
    return [{
      id: null,
      name: '(new group)',
      active: true,
      application: null,
      cronExpression: get(this, 'cronOptions')[0].cron,
      emailConfig: {
        functionIds: []
      }
    }].concat(res.sortBy('name'));
  },

  /**
   * Parses anomaly function entities into power-select options
   *
   * @param res {Array} ANOMALY_FUNCTION entities
   * @returns {Array} function options
   * @private
   */
  _makeFunctionOptions (res) {
    return res.map(f => Object.assign({}, { name: f.functionName, id: f.id })).sortBy('name');
  },

  actions: {
    /**
     * Handles recipient updates
     */
    onRecipients () {
      const recipients = get(this, 'groupRecipients');
      set(this, 'group.recipients', recipients.replace(' ', ''));
    },

    /**
     * Handles functionIds selection
     * @param functionOption {Array} function options
     */
    onFunctionIds (functionOptions) {
      const functionIds = functionOptions.map(opt => opt.id);
      set(this, 'group.emailConfig.functionIds', functionIds);
    },

    /**
     * Handles the close event
     */
    onCancel () {
      const onExit = get(this, 'onExit');

      setProperties(this, { group: null, application: null, groupRecipients: null, isShowingModal: false });

      if (onExit) { onExit(); }
    },

    /**
     * Handles save event
     */
    onSubmit () {
      const { group, onSave } = getProperties(this, 'group', 'onSave');

      // TODO fix modal still hiding regardless of input validation results

      if (_.isEmpty(group)) {
        set(this, 'message', 'Please select a group first');
        return;
      }

      if (_.isEmpty(group.name) || group.name.startsWith('(')) {
        set(this, 'message', 'Please enter a valid group name');
        return;
      }

      // TODO use ember data API for alert configs
      fetch('/thirdeye/entity?entityType=ALERT_CONFIG', { method: 'POST', body: JSON.stringify(group) })
        .then(checkStatus)
        .then(res => setProperties(this, {
          isShowingModal: false,
          application: null,
          group: null,
          groupRecipients: null
        }))
        .then(res => {
          if (onSave) { onSave(group); }
        })
        .catch(err => set(this, 'message', err));
     },

    /**
     * Handles group selection
     * @param group
     */
    onGroup (group) {
      setProperties(this, {
        group,
        groupFunctionIds: (group.emailConfig.functionIds || []).join(', '),
        groupRecipients: (group.recipients || '').split(',').join(', ')
      });
    },

    /**
     * Handles copy button
     */
    onCopy () {
      const { group, groupOptions } = getProperties(this, 'group', 'groupOptions');

      if (_.isEmpty(group) || group.id === null) { return; }

      const newGroup = Object.assign(_.cloneDeep(group), {
        id: null,
        name: `Copy of ${group.name}`
      });

      groupOptions.pushObject(newGroup);

      setProperties(this, { group: newGroup });
    },

    /**
     * Handles cron selection
     * @param cronOption {object} cron option
     */
    onCron (cronOption) {
      set(this, 'group.cronExpression', cronOption.cron);
    },

    /**
     * Handles application selection
     * @param applicationOption {object} application option
     */
    onApplication (applicationOption) {
      setProperties(this, { application: applicationOption.application, group: null, groupRecipients: null });
    }
  }
});
