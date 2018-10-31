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
 * @property {Function} onSubmit          - closure action saving group changes
 * @property {Function} onCancel          - closure action cancelling the modal
 * @property {int} preselectedGroupId     - preselected group id (optional)
 * @property {int} preselectedFunctionId  - preselected group id (optional)
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
import RSVP from 'rsvp';

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
   * id of preselected group
   * @type {int}
   */
  preselectedGroupId: null,

  /**
   * id of preselected function
   * @type {int}
   */
  preselectedFunctionId: null,

  /**
   * Action for save/confirm. Passes selected group as argument
   * @type {Function}
   */
  onSave: null,

  /**
   * Action for cancellation.
   * @type {Function}
   */
  onExit: null,

  /**
   * Cache for current group
   * @type {object}
   */
  changeCacheCurrent: null,

  /**
   * Cache for all changed groups
   * @type {Set}
   */
  changeCache: null,

  /**
   * Cache for existing group names
   * @type {Set}
   */
  groupNameCache: computed('groupOptionsRaw', function () {
    const groupOptionsRaw = get(this, 'groupOptionsRaw');
    return new Set(groupOptionsRaw.map(opt => opt.name));
  }),

  /**
   * Handle init of properties
   */
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
      .then(options => {
        this._handlePreselection(options, get(this, 'preselectedGroupId'));
        return options;
      })
      .then(options => set(this, 'groupOptionsRaw', options));

    // TODO use ember data API for anomaly functions
    // TODO support incremental backend search
    fetch('/thirdeye/entity/ANOMALY_FUNCTION')
      .then(checkStatus)
      .then(res => this._makeFunctionOptions(res))
      .then(options => set(this, 'functionOptions', options));

    // TODO optimize reverse lookup of options via dict if necessary

    set(this, 'changeCache', new Set());
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
   * Preselected function name, resolved via functionOptions
   * @type {string}
   */
  functionName: computed('functionOptions', 'preselectedFunctionId', function () {
    const { functionOptions, preselectedFunctionId } =
      getProperties(this, 'functionOptions', 'preselectedFunctionId');

    if (!_.isNumber(preselectedFunctionId)) { return; }

    const opt = functionOptions.find(opt => opt.id === preselectedFunctionId);

    if (_.isEmpty(opt)) { return; }

    return opt.name;
  }),

  /**
   * Flag for preselected function
   * @type {boolean}
   */
  hasFunction: computed('functionName', function () {
    return _.isNumber(get(this, 'preselectedFunctionId'));
  }),

  /**
   * Flag for add function shortcut
   * @type {boolean}
   */
  hasFunctionShortcut: computed('preselectedFunctionId', 'group', function () {
    const { preselectedFunctionId, group } =
      getProperties(this, 'preselectedFunctionId', 'group');

    if (_.isEmpty(group) || !_.isNumber(preselectedFunctionId)) { return false; }

    if (group.emailConfig.functionIds.includes(preselectedFunctionId)) {
      return false;
    }

    return true;
  }),

  /**
   * Footer text tracking change cache
   * @type {string}
   */
  footerText: computed('changeCache', function () {
    const changeCache = get(this, 'changeCache');
    if (_.isEmpty(changeCache)) {
      return '';
    }
    if (changeCache.size === 1) {
      return '(1 group will be modified)';
    }
    return `(${changeCache.size} groups will be modified)`;
  }),

  /**
   * Error message container for group name
   * @type {string}
   */
  groupNameMessage: null,

  /**
   * Error message container for to-recipients
   * @type {string}
   */
  toAddrWarning: null,

  /**
   * Temp storage for recipients(to) from text input
   * @type {String}
   */
  toAddresses: null,

  /**
   * Temp storage for recipients(cc) from text input
   * @type {String}
   */
  ccAddresses: null,

  /**
   * Temp storage for recipients(bcc) from text input
   * @type {String}
   */
  BccAddresses: null,

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
    const options = groupOptionsRaw.filter(opt => opt.application === null || opt.application === application || opt.id === null);
    options[0].application = application; // set application for "new" template
    return options;
  }),

  /**
   * Shortcut header for groups an alert is already part of
   * @type {Array}
   */
  groupShortcuts: computed('preselectedFunctionId', 'groupOptionsRaw', function () {
    const { preselectedFunctionId, groupOptionsRaw } =
      getProperties(this, 'preselectedFunctionId', 'groupOptionsRaw');

    return groupOptionsRaw.filter((group) => {
      if (group.emailConfig && group.emailConfig.functionIds) {
        return group.emailConfig.functionIds.includes(preselectedFunctionId);
      }
    });
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
  applicationOptionSelected: computed('application', function () {
    const { applicationOptions, application } = getProperties(this, 'applicationOptions', 'application');
    if (_.isEmpty(application)) { return; }
    return applicationOptions.find(opt => opt.application === application);
  }).readOnly(),

  /**
   * Cron options available for selection
   * @type {Array}
   */
  cronOptionsRaw: [
    { name: 'immediately', cron: '0 0/5 * * * ? *' },
    { name: 'every 15 min', cron: '0 0/15 * * * ? *' },
    { name: 'every hour', cron: '0 0 * * * ? *' },
    { name: 'every 3 hours', cron: '0 0 0/3 * * ? *' },
    { name: 'every 6 hours', cron: '0 0 0/6 * * ? *' },
    { name: 'daily', cron: '0 0 0 * * ? *' },
    { name: 'weekly', cron: '0 0 0 * * SUN *' }
  ],

  /**
   * Cron options including an optional custom, pre-existing option
   * @type {Array}
   */
  cronOptions: computed('group', 'cronOptionsRaw', function () {
    const { group, cronOptionsRaw } = getProperties(this, 'group', 'cronOptionsRaw');

    if (_.isEmpty(group)) { return cronOptionsRaw; }

    if (_.isEmpty(cronOptionsRaw.find(opt => opt.cron === group.cronExpression))) {
      return [...cronOptionsRaw].concat([{ name: `custom setting ("${group.cronExpression}")`, cron: group.cronExpression  }]);
    }

    return cronOptionsRaw;
  }),

  /**
   * Currently selected cron option
   * @type {object}
   */
  cronOptionSelected: computed('group.cronExpression', function () {
    const { cronOptions, group } = getProperties(this, 'cronOptions', 'group');
    if (_.isEmpty(group)) { return; }
    return cronOptions.find(opt => opt.cron === group.cronExpression);
  }).readOnly(),

  /**
   * SubjectType options available for selection
   * @type {Array}
   */
  subjectTypeOptions: [
    { name: 'group name only', subjectType: 'ALERT' },
    { name: 'with metric name', subjectType: 'METRICS' },
    { name: 'with dataset name', subjectType: 'DATASETS' }
  ],

  /**
   * Currently selected subjectType option
   * @type {object}
   */
  subjectTypeOptionSelected: computed('group.subjectType', function () {
    const { subjectTypeOptions, group } = getProperties(this, 'subjectTypeOptions', 'group');
    if (_.isEmpty(group)) { return; }
    return subjectTypeOptions.find(opt => opt.subjectType === group.subjectType);
  }).readOnly(),

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
  functionOptionsSelected: computed('functionOptionsSelectedIds', function () {
    const { functionOptions, functionOptionsSelectedIds } = getProperties(this, 'functionOptions', 'functionOptionsSelectedIds');
    if (_.isEmpty(functionOptionsSelectedIds)) { return []; }
    return functionOptions.filter(opt => functionOptionsSelectedIds.has(opt.id));
  }).readOnly(),

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
      subjectType: get(this, 'subjectTypeOptions')[0].subjectType,
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

  /**
   * Selects the group with given id
   *
   * @param groupOptions {Array} available groups
   * @param groupId {int} preselected group id
   * @private
   */
  _handlePreselection(groupOptions, groupId) {
    if (_.isEmpty(groupId)) { return; }

    const group = groupOptions.find(opt => opt.id === groupId);
    if (_.isEmpty(group)) { return; }

    setProperties(this, { group, application: group.application });
  },

  /**
   * Stage a newly selected group for caching if not already tracked in change cache
   *
   * @param group {object} group
   * @private
   */
  _makeChangeCacheCurrent(group) {
    const changeCache = get(this, 'changeCache');

    if (_.isEmpty(group)) { return; }

    if (changeCache.has(group)) {
      set(this, 'changeCacheCurrent', group);
      return;
    }

    set(this, 'changeCacheCurrent', _.cloneDeep(group));
  },

  /**
   * Checks for changes of the selected group and adds it to the change cache if verified
   * // TODO trigger this (debounced) on every edit?
   *
   * @private
   */
  _updateChangeCache() {
    const { changeCacheCurrent, changeCache, group } =
      getProperties(this, 'changeCacheCurrent', 'changeCache', 'group');

    if (_.isEmpty(group)) { return; }

    if (!changeCache.has(group) && !_.isEqual(changeCacheCurrent, group)) {
      const newChangeCache = new Set([...changeCache].concat([group]));
      set(this, 'changeCache', newChangeCache);
    }
  },

  /**
   * Validate the group name for validity and uniqueness and display warning if necessary.
   *
   * @param group {object} group
   * @private
   */
  _validateGroupName(group) {
    const { groupNameCache, changeCacheCurrent } =
      getProperties(this, 'groupNameCache', 'changeCacheCurrent');

    if (_.isEmpty(group)) { return; }
    if (_.isEmpty(groupNameCache)) { return; }

    const inUse = new Set(groupNameCache);
    if (!_.isEmpty(changeCacheCurrent)) {
      inUse.delete(changeCacheCurrent.name);
    }

    if (_.isEmpty(group.name) || group.name.startsWith('(') || inUse.has(group.name)) {
      set(this, 'groupNameMessage', 'group name invalid or already in use');
    } else {
      set(this, 'groupNameMessage', null);
    }
  },

  /**
   * Check for at least one recipient
   *
   * @param toAddr {String} comma separated email addresses
   * @private
   */
  _validateRecipient(toAddr) {
    if (!_.isEmpty(toAddr)) {
      var emailArray = toAddr.split(',');
      for (var index = 0; index < emailArray.length; index++) {
        if (this._validateEmail(emailArray[index].trim())) {
          set(this, 'toAddrWarning', null);
          return;
        }
      }
    }

    set(this, 'toAddrWarning', 'Please enter at least one valid recipient');
  },

  /**
   * Verify if string is a valid email address
   *
   * @param email {String} email address
   * @private
   */
  _validateEmail(email) {
    var re = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    return re.test(String(email).toLowerCase());
  },

  actions: {
    /**
     * Handles recipient updates
     */
    onToAddresses () {
      const toAddresses = get(this, 'toAddresses');
      this._validateRecipient(toAddresses);

      set(this, 'group.receiverAddresses.to', toAddresses.replace(/[^!-~]+/g, '').replace(/,+/g, ',').split(','));
    },
    onCcAddresses () {
      const ccAddresses = get(this, 'ccAddresses');
      set(this, 'group.receiverAddresses.cc', ccAddresses.replace(/[^!-~]+/g, '').replace(/,+/g, ',').split(','));
    },
    onBccAddresses () {
      const bccAddresses = get(this, 'bccAddresses');
      set(this, 'group.receiverAddresses.bcc', bccAddresses.replace(/[^!-~]+/g, '').replace(/,+/g, ',').split(','));
    },

    /**
     * Handles functionIds selection
     * @param functionOptions {Array} function options
     */
    onFunctionIds (functionOptions) {
      const prevFunctionIds = get(this, 'group.emailConfig.functionIds');
      const fid = get(this, 'preselectedFunctionId');

      const functionIds = functionOptions.map(opt => opt.id);
      set(this, 'group.emailConfig.functionIds', functionIds);

      // trigger group shortcut compute (instead of deep nested listener)
      if (_.isNumber(fid) && (prevFunctionIds.includes(fid) || functionIds.includes(fid))) {
        this.notifyPropertyChange('preselectedFunctionId');
      }
    },

    /**
     * Handles adding preselected function id to selected function ids
     */
    onFunctionShortcut () {
      const { preselectedFunctionId, group } =
        getProperties(this, 'preselectedFunctionId', 'group');

      if (!_.isNumber(preselectedFunctionId)) { return; }
      if (_.isEmpty(group)) { return; }

      const functionIds = new Set(group.emailConfig.functionIds);
      functionIds.add(preselectedFunctionId);

      set(this, 'group.emailConfig.functionIds', [...functionIds]);

      // trigger group shortcut compute (instead of deep nested listener)
      this.notifyPropertyChange('preselectedFunctionId');
    },

    /**
     * Handles the close event
     */
    onCancel () {
      const onExit = get(this, 'onExit');

      setProperties(this, {
        application: null,
        group: null,
        toAddresses: null,
        ccAddresses: null,
        bccAddresses: null,
        showManageGroupsModal: false,
        changeCache: new Set(),
        groupNameMessage: null,
        toAddrWarning: null
      });

      if (onExit) { onExit(); }
    },

    /**
     * Handles save event
     */
    onSubmit () {
      this._updateChangeCache();

      const { group, onSave, changeCache } = getProperties(this, 'group', 'onSave', 'changeCache');

      const promises = [...changeCache].map(group => {
        fetch('/thirdeye/entity?entityType=ALERT_CONFIG', {method: 'POST', body: JSON.stringify(group)})
          .then(checkStatus);
      });

      // TODO use ember data API for alert configs
      RSVP.allSettled(promises)
        .then(res => setProperties(this, {
          application: null,
          group: null,
          toAddresses: null,
          ccAddresses: null,
          bccAddresses: null,
          showManageGroupsModal: false,
          changeCache: new Set(),
          groupNameMessage: null,
          toAddrWarning: null
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
      this._updateChangeCache();
      this._makeChangeCacheCurrent(group);

      setProperties(this, {
        application: group.application, // in case not set
        group,
        groupFunctionIds: (group.emailConfig.functionIds || []).join(', '),
        toAddresses: (group.receiverAddresses.to || []).join(', '),
        ccAddresses: (group.receiverAddresses.cc || []).join(', '),
        bccAddresses: (group.receiverAddresses.bcc || []).join(', ')
      });

      this._validateGroupName(group);
    },

    /**
     * Handles copy button
     */
    onCopy () {
      const { group, groupOptions, groupOptionsRaw } = getProperties(this, 'group', 'groupOptions', 'groupOptionsRaw');

      if (_.isEmpty(group) || group.id === null) { return; }

      const newGroup = Object.assign(_.cloneDeep(group), {
        id: null,
        name: `Copy of ${group.name}`
      });

      // push options directly rather than triggering re-compute
      groupOptionsRaw.pushObject(newGroup);
      groupOptions.pushObject(newGroup);

      setProperties(this, { group: newGroup, changeCacheCurrent: group });
      this._validateGroupName(newGroup);
    },

    /**
     * Handles cron selection
     * @param cronOption {object} cron option
     */
    onCron (cronOption) {
      set(this, 'group.cronExpression', cronOption.cron);
    },

    /**
     * Handles subjectType selection
     * @param subjectOption {object} subjectType option
     */
    onSubjectType (subjectOption) {
      set(this, 'group.subjectType', subjectOption.subjectType);
    },

    /**
     * Handles application selection
     * @param applicationOption {object} application option
     */
    onApplication (applicationOption) {
      this._updateChangeCache();
      setProperties(this, {
        application: applicationOption.application,
        group: null,
        toAddresses: null,
        ccAddresses: null,
        bccAddresses: null,
        groupNameMessage: null,
        toAddrWarning: null
      });
    },

    /**
     * Handle group name edit
     */
    onName () {
      const group = get(this, 'group');
      this._validateGroupName(group);
    }
  }
});
