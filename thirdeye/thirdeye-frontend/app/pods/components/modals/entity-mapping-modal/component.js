/**
 * Component displaying the entity mapping inside a modal
 * @module components/modals/entity-mapping-modal
 * @property {Boolean} showEntityMappingModal  - Flag toggling the modal view
 * @property {Object} metric                   - primary metric Object
 * @property {Function} onSubmit               - closure action that handles the submit events
 * @example
    {{modals/entity-mapping-modal
      showEntityMappingModal=showEntityMappingModal
      metric=(get entities metricUrn)
      onSubmit=(action "onModalSubmit")
    }}
 * @exports entity-mapping-modal
 * @author yyuen
 */

import Component from '@ember/component';
import { get, set, getProperties, setProperties, computed, getWithDefault } from '@ember/object';
import { reads } from '@ember/object/computed';
import { later } from '@ember/runloop';
import { inject as service } from '@ember/service';
import { isPresent } from '@ember/utils';

import fetch from 'fetch';
import { task, timeout } from 'ember-concurrency';
import { checkStatus, postProps } from 'thirdeye-frontend/utils/utils';
import { deleteProps } from 'thirdeye-frontend/utils/constants';
import _ from 'lodash';

const MAPPING_TYPES = ['METRIC', 'DIMENSION', 'DATASET', 'LIXTAG'];

import entityMappingApi from 'thirdeye-frontend/utils/api/entity-mapping';

export default Component.extend({
  session: service(),
  /**
   * Default selected mapping type
   * @type {String}
   */
  selectedMappingType: 'dataset',

  /**
   * User selected entity to be mapped
   * @type {String|Object}
   */
  selectedEntity: '',

  /**
   * Mapping type array used for the mapping drop down
   * @type {Array}
   */
  mappingTypes: MAPPING_TYPES.map((type) => type.toLowerCase()).sort(),

  /**
   * Mapping from urn to entities Ids
   * @type {Object}
   */
  urnToId: Object.assign({}),

  /**
   * Error Message String
   */
  errorMessage: '',

  /**
   * Cached private property for the id portion of the urn
   */
  _id: '',

  /**
   * passed primary metric attrs
   * @type {Object}
   */
  metric: null,

  /**
   * Cached metric
   */
  _cachedMetric: null,

  /**
   * Last entity searched
   * @type {String}
   */
  lastSearchTerm: '',

  /**
   * Fetched related entities
   * @type {Array}
   */
  _relatedEntities: Object.assign([]),

  /**
   * Flag for displaying the modal
   * @type {boolean}
   */
  showEntityMappingModal: true,

  /**
   * current logged in user
   */
  user: reads('session.data.authenticated.name'),

  /**
   * Header text of modal
   * @type {string}
   */
  headerText: computed('metric', function () {
    const metric = get(this, 'metric');
    if (_.isEmpty(metric)) {
      return 'Configure Filters';
    }
    return `Configure Filters for analyzing ${metric.label}`;
  }),

  /**
   * Primary metric urn
   */
  metricUrn: computed('_cachedMetric', function () {
    const cachedMetricUrn = getWithDefault(this, '_cachedMetric.urn', '');

    const [app, metric, id] = cachedMetricUrn.split(':');

    return [app, metric, id].join(':');
  }),

  /**
   * Applies data massaging while getting the property
   * @type {Array}
   */
  relatedEntities: computed('_relatedEntities.@each', {
    get() {
      return getWithDefault(this, '_relatedEntities', []);
    },
    set(key, value) {
      return value;
    }
  }),

  /**
   * Determines whether to show the power-select or the input
   * @type {Boolean}
   */
  showAdvancedInput: computed('selectedMappingType', function () {
    const selectedMappingType = get(this, 'selectedMappingType');

    return ['dataset', 'metric', 'service'].includes(selectedMappingType);
  }),

  /**
   * Calculates the correct urn prefix
   * @type {String}
   */
  urnPrefix: computed('selectedMappingType', function () {
    return `thirdeye:${this.get('selectedMappingType')}:`;
  }),

  /**
   * Single source of truth for new entity mapping urn
   * this cp gets and sets 'urnPrefix' and '_id' if the syntax is correct
   * @type {String}
   */
  urn: computed('urnPrefix', '_id', {
    get() {
      return `${this.get('urnPrefix')}${this.get('_id')}`;
    },
    set(key, value) {
      if (value.startsWith(this.get('urnPrefix'))) {
        const newUrn = value.split(':').pop();
        this.set('_id', newUrn);
      }
      return value;
    }
  }),

  /**
   * Checks if the currently selected entity is already in the mapping
   */
  mappingExists: computed('selectedEntity.label', 'relatedEntities.@each.alias', 'metric', 'urn', function () {
    const { selectedEntity: entity, relatedEntities, metric, urn } = getProperties(
      this,
      'selectedEntity',
      'relatedEntities',
      'metric',
      'urn'
    );

    return [metric, ...relatedEntities].some((relatedEntity) => {
      return relatedEntity.label === entity.alias || relatedEntity.urn === urn;
    });
  }),

  /**
   * Helper function that sets an error
   * @param {String} error - the error message
   */
  setError(error) {
    set(this, 'errorMessage', error);
  },

  /**
   * Helper function that clears errors
   */
  clearError() {
    set(this, 'errorMessage', '');
  },

  /**
   * Entity Mapping columns to be passed into
   * ember-models-table
   */
  entityColumns: Object.freeze([
    {
      propertyName: 'type',
      title: 'Types',
      filterWithSelect: true,
      className: 'te-modal__table-cell te-modal__table-cell--capitalized',
      predefinedFilterOptions: MAPPING_TYPES.map((type) => type.toLowerCase())
    },
    {
      template: 'custom/filterLabel',
      title: 'Filter value',
      className: 'te-modal__table-cell',
      propertyName: 'label',
      sortedBy: 'label',
      // custom filter function
      filterFunction(cell, string, record) {
        return ['urn', 'label'].some((key) => record[key].includes(string));
      }
    },
    {
      propertyName: 'createdBy',
      title: 'Created by',
      className: 'te-modal__table-cell'
    },
    // Todo: Fix back end to send
    // dateCreated data
    // {
    //   propertyName: 'dateCreated',
    //   title: 'Date Created'
    // },
    {
      template: 'custom/tableDelete',
      title: '',
      className: 'te-modal__table-cell te-modal__table-cell--delete te-modal__table-cell--dark'
    }
  ]),

  /**
   * Custom classes to be applied to the entity modal table
   */
  classes: Object.freeze({
    theadCell: 'te-modal__table-header'
  }),

  /**
   * Whether the user can add the currently selected entity
   */
  canAddMapping: computed('mappingExists', 'selectedEntity', '_id', function () {
    const { mappingExists, _id, metric } = getProperties(this, 'mappingExists', '_id', 'metric');
    return !mappingExists && [_id, metric].every(isPresent);
  }),

  /**
   * Calculates the params object
   * to be send with the create call
   * @type {Object}
   */
  entityParams: computed('selectedMappingType', 'metricUrn', 'urn', function () {
    const { selectedMappingType: entityType, metricUrn, urn } = getProperties(
      this,
      'metricUrn',
      'selectedMappingType',
      'urn'
    );

    return {
      fromURN: metricUrn,
      mappingType: `METRIC_TO_${entityType.toUpperCase()}`,
      score: '1.0',
      toURN: urn
    };
  }),

  /**
   * Restartable Ember concurrency task that triggers the autocomplete behavior
   * @return {Array}
   */
  searchEntitiesTask: task(function* (searchString) {
    yield timeout(600);
    const entityType = this.get('selectedMappingType');
    let url = '';

    switch (entityType) {
      case 'metric':
        url = entityMappingApi.metricAutoCompleteUrl(searchString);
        break;

      case 'dataset':
      case 'service':
        return this.get(`${entityType}s`).filter((item) => item.includes(searchString));
    }

    /**
     * Necessary headers for fetch
     */
    const headers = {
      method: 'GET',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        Cache: 'no-cache'
      },
      credentials: 'include'
    };

    return fetch(url, headers).then(checkStatus);
  }).restartable(),

  async init() {
    this._super(...arguments);
    const datasets = await fetch(entityMappingApi.getDatasetsUrl).then(checkStatus);

    if (this.isDestroyed || this.isDestroying) {
      return;
    }

    set(this, 'datasets', datasets);
  },

  /**
   * Fetches new entities with caching and diff checking
   */
  didReceiveAttrs() {
    const { metric, _cachedMetric, showEntityMappingModal } = getProperties(
      this,
      'metric',
      '_cachedMetric',
      'showEntityMappingModal'
    );

    if (showEntityMappingModal && metric && !_.isEqual(metric, _cachedMetric)) {
      set(this, '_cachedMetric', metric);
      this._fetchRelatedEntities();
    }
  },

  /**
   * Reset the entity mapping type and selected entity
   * @param {String} selectedMappingType - selected mapping type
   * @return {undefined}
   */
  reset(selectedMappingType = 'dataset') {
    this.setProperties({
      selectedMappingType,
      selectedEntity: '',
      _id: ''
    });
  },

  /**
   * Build the Urn to Id mapping
   * @param {Array} entities - array of entities
   * @return {undefined}
   */
  buildUrnToId(entities) {
    const urnToId = entities.reduce((agg, entity) => {
      agg[entity.toURN] = entity.id;
      return agg;
    }, {});
    set(this, 'urnToId', urnToId);
  },

  /**
   * Construct the url string based on the passed entities
   * @param {Array} entities - array of entities
   * @return {String}
   */
  makeUrlString(entities) {
    const urnStrings = entities.map((entity) => `${entity.toURN}`).join(',');

    if (urnStrings.length) {
      return entityMappingApi.getRelatedEntitiesDataUrl(urnStrings);
    }
  },

  /**
   * Fetches related Entities
   */
  _fetchRelatedEntities: async function () {
    const metricUrn = get(this, 'metricUrn');
    const entities = await fetch(`${entityMappingApi.getRelatedEntitiesUrl}/${metricUrn}`).then(checkStatus);
    const url = this.makeUrlString(entities);
    this.buildUrnToId(entities);

    if (!url) {
      return;
    }
    const relatedEntities = await fetch(url).then(checkStatus);

    // merges createBy Props
    relatedEntities.forEach((item) => {
      if (!item.urn) {
        return;
      }
      const { createdBy } = _.find(entities, { toURN: item.urn }) || { createdBy: null };
      item.createdBy = createdBy;
      return item;
    });

    set(this, '_relatedEntities', relatedEntities);
  },

  actions: {
    /**
     * Handles the close event
     * @return {undefined}
     */
    onExit() {
      set(this, 'showEntityMappingModal', false);
      this.onSubmit();
    },

    /**
     * Deletes the entity
     * @param {Object} entity - entity to delete
     * @return {undefined}
     */
    onDeleteEntity: async function (entity) {
      const relatedEntities = get(this, 'relatedEntities');
      const id = get(this, 'urnToId')[entity.urn];
      const url = `${entityMappingApi.deleteUrl}/${id}`;

      try {
        const res = await fetch(url, deleteProps);
        const { status } = res;
        if (status !== 200) {
          throw new Error('Uh Oh. Something went wrong.');
        }
      } catch (error) {
        return this.setError('error');
      }

      this.clearError();
      relatedEntities.removeObject(entity);
    },

    /**
     * Handles the add event
     * sends new mapping and reloads
     * @return {undefined}
     */
    onAddFilter: async function () {
      const { canAddMapping, entityParams } = getProperties(this, 'canAddMapping', 'entityParams');

      if (!canAddMapping) {
        return;
      }

      try {
        const res = await fetch(entityMappingApi.createUrl, postProps(entityParams));
        const { status } = res;
        if (status !== 200) {
          throw new Error('Uh Oh. Something went wrong.');
        }
      } catch (error) {
        return this.setError('error');
      }
      await this._fetchRelatedEntities();
      this.reset();
      this.clearError();
    },

    /**
     * Action handler for metric selection change
     * @param {Object} metric
     */
    onEntitySelection(entity) {
      setProperties(this, {
        selectedEntity: entity,
        _id: entity.id || entity
      });

      // toggles the tooltip view
      set(this, 'showTooltip', true);
      later(() => {
        set(this, 'showTooltip', false);
      }, 2000);
    },

    /**
     * Performs a search task while cancelling the previous one
     * @param {Array} metrics
     */
    onSearch(searchString) {
      const { lastSearchTerm, searchEntitiesTask: task } = getProperties(this, 'lastSearch', 'searchEntitiesTask');

      searchString = searchString.length ? searchString : lastSearchTerm;
      const taskInstance = task.perform(searchString);

      this.set('lastSearchTerm', searchString);
      return taskInstance;
    },

    /**
     * Action handler validating the custom urn strings
     * @param {Object} metric
     */
    onKeyPress() {
      const { urn, urnPrefix } = getProperties(this, 'urn', 'urnPrefix');

      if (!urn.startsWith(urnPrefix)) {
        this.set('urn', urnPrefix);
      }
    },

    /**
     * Clears the selected Entity and sets new mapping
     * @return {undefined}
     */
    onEntityMappingChange(selectedMappingType) {
      this.reset(selectedMappingType);
    }
  }
});
