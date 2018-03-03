/**
 * Component displaying the entity mapping inside a modal
 * @module components/modals/entity-mapping-modal
 * @property {Boolean} showEntityMapping  - Flag toggling the modal view
 * @property {Object} metric              - primary metric Object
 * @property {Function} onSubmit          - closure action that handles the submit events
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
import { inject as service } from '@ember/service';
import { later } from '@ember/runloop';
import fetch from 'fetch';
import { task, timeout } from 'ember-concurrency';
import { checkStatus, postProps } from 'thirdeye-frontend/utils/utils';
import { deleteProps } from 'thirdeye-frontend/utils/constants';
import _ from 'lodash';

const MAPPING_TYPES = [
  'METRIC',
  'DIMENSION',
  'SERVICE',
  'DATASET',
  'LIXTAG',
  'CUSTOM'
];

export default Component.extend({
  session: service(),
  /**
   * Default selected mapping type
   */
  selectedMappingType: 'metric',

  /**
   * User selected entity to be mapped
   */
  selectedEntity: {},

  /**
   * Mapping type array used for the mapping drop down
   */
  mappingTypes: MAPPING_TYPES.map(type => type.toLowerCase()),

  /**
   * Save the search task
   */
  mostRecentSearch: null,

  /**
   * Mapping from urn to entities Ids
   */
  urnToId: {},

  /**
   * Error Message String
   */
  errorMessage: '',


  /**
   * isDirty flag that determines if a changed occurs
   */
  hasNewMapping: false,

  /**
   * current logged in user
   */
  user: computed.reads('session.data.authenticated.name'),

  /**
   * passed primary metric attrs
   */
  metric: null,

  /**
   * Cached metric
   */
  _cachedMetric: null,

  /**
   * Determines if the mapping type is metric
   * @returns {Boolean}
   */
  isMetricEntityType: computed.equal('selectedMappingType', 'metric'),

  /**
   * Primary metric urn
   */
  metricUrn: computed('_cachedMetric', function() {
    const cachedMetricUrn = getWithDefault(this, '_cachedMetric.urn', '');

    const [app, metric, id] = cachedMetricUrn.split(':');

    return [app, metric, id].join(':');
  }),

  /**
   * Fetched related entities
   */
  _relatedEntities: [],

  /**
   * Applies data massaging while getting the property
   */
  relatedEntities: computed(
    '_relatedEntities.@each',
    'user', {
      get() {
        const {
          user,
          _relatedEntities = []
        } = getProperties(
          this,
          'user',
          '_relatedEntities');

        return _relatedEntities.map((entity) => {
          return this.formatEntity(entity, user);
        });
      },
      set(key, value) {
        return value;
      }
    }
  ),

  /**
   * Data masssages the entity based on the user
   * @param {Object} entity - the related entity
   * @param {String} user   - the current logged in user
   */
  formatEntity(entity, user) {
    // Todo: allow all users to edit for now since
    // we do not have consistent data

    // const { createdBy = 'unkown' } = entity;
    // user = user || get(this, 'user');
    // set(entity, 'isDeletable', createdBy === user);
    set(entity, 'isDeletable', true);

    return entity;
  },

  /**
   * Build the Urn to Id mapping
   * @param {Array} entities - array of entities
   */
  builUrnToId(entities) {
    const urnToId = entities.reduce((agg, entity) => {
      agg[entity.toURN] = entity.id;
      return agg;
    }, {});
    set(this, 'urnToId', urnToId);
  },

  /**
   * Construct the url string based on the passed entities
   * @param {Array} entities - array of entities
   */
  makeUrlString(entities) {
    const urnStrings = entities
      .map((entity) => `${entity.toURN}`)
      .join(',');

    if (!urnStrings.length) {
      return;
    }

    return `/rootcause/raw?framework=identity&urns=${urnStrings}`;
  },

  /**
   * Fetches related Entities
   */
  _fetchRelatedEntities: async function() {
    const metricUrn = get(this, 'metricUrn');

    const entities = await fetch(`/entityMapping/view/fromURN/${metricUrn}`).then(checkStatus);
    const url = this.makeUrlString(entities);
    this.builUrnToId(entities);

    if (!url) {
      return;
    }
    const relatedEntities = await fetch(url).then(checkStatus);

    // merges createBy Props
    relatedEntities.map((item) => {
      if (!item.urn) {
        return;
      }
      const { createdBy } = _.find(entities, { toURN: item.urn });
      item.createdBy = createdBy;
      return item;
    });

    set(this, '_relatedEntities', relatedEntities);
  },

  /**
   * Fetches new entities with caching and diff checking
  */
  didReceiveAttrs() {
    const {
      metric,
      _cachedMetric,
      showEntityMappingModal
    } = getProperties(this, 'metric', '_cachedMetric', 'showEntityMappingModal');

    if (showEntityMappingModal && metric && !_.isEqual(metric, _cachedMetric)) {
      set(this, '_cachedMetric', metric);

      this._fetchRelatedEntities();
    }
  },

  /**
   * Checks if the currently selected entity is already in the mapping
   */
  mappingExists: computed(
    'selectedEntity.label',
    'relatedEntities.@each.alias',
    'metric',
    function() {
      const {
        selectedEntity: entity,
        relatedEntities,
        metric
      } = getProperties(this, 'selectedEntity', 'relatedEntities', 'metric');

      return (metric.label === entity.alias) || relatedEntities.some((relatedEntity) => {
        relatedEntity.label === entity.alias;
      });
    }
  ),

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
  entityColumns: [
    {
      propertyName: 'type',
      title: 'Types',
      filterWithSelect: true,
      className: 'te-modal__table-cell te-modal__table-cell--capitalized',
      predefinedFilterOptions: [
        'Metric',
        'Other'
      ]
    },
    {
      template: 'custom/filterLabel',
      title: 'Filter value',
      className: 'te-modal__table-cell',
      propertyName: 'label',
      sortedBy: 'label',
      // custom filter function
      filterFunction(cell, string, record) {
        return ['urn', 'label'].some(key  => record[key].includes(string));
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
  ],

  /**
   * Custom classes to be applied to the entity modal table
   */
  classes: {
    theadCell: "te-modal__table-header"
  },

  /**
   * Ember concurrency task that triggers the metric autocomplete
   */
  searchEntities: task(function* (searchText) {
    yield timeout(600);

    let url = `/data/autocomplete/metric?name=${searchText}`;

    /**
     * Necessary headers for fetch
     */
    const headers = {
      method: "GET",
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Cache': 'no-cache'
      },
      credentials: 'include'
    };

    return fetch(url, headers)
      .then(checkStatus);
  }),

  actions: {

    /**
     * Handles the close event
     */
    onExit() {
      this.attrs.onSubmit();
    },

    /**
     * Deletes the entity
     * @param {Object} entity - entity to delete
     */
    onDeleteEntity: async function(entity) {
      if (entity.isDeletable) {
        const relatedEntities = get(this, 'relatedEntities');
        const id = get(this, 'urnToId')[entity.urn];
        const url = `/entityMapping/delete/${id}`;

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
        set(this, 'hasNewMapping', true);
      }
    },

    /**
     * Handles the add event
     * sends new mapping and reloads
     */
    onAddFilter: async function() {
      const {
        selectedEntity: entity,
        mappingExists,
        metric
      } = getProperties(
        this,
        'selectedEntity',
        'relatedEntities',
        'user',
        'mappingExists',
        'metric'
      );

      if (!metric && mappingExists) { return; }

      const url = `/entityMapping/create`;
      const metricUrn = get(this, 'metricUrn');
      const params = {
        fromURN: metricUrn,
        mappingType: 'METRIC_TO_METRIC',
        score: '1.0',
        toURN: `thirdeye:metric:${entity.id}`
      };

      try {
        const res = await fetch(url, postProps(params));
        const { status } = res;
        if (status !== 200) {
          throw new Error('Uh Oh. Something went wrong.');
        }
      } catch (error) {
        return this.setError('error');
      }
      await this._fetchRelatedEntities();
      set(this, 'selectedEntity', {});
      set(this, 'hasNewMapping', true);
      this.clearError();
    },

    /**
     * Action handler for metric selection change
     * @param {Object} metric
     */
    onEntitySelection(entity) {
      setProperties(this, {
        selectedEntity: entity,
        showTooltip: true
      });

      // toggles the tooltip view
      set(this, 'selectedEntity', entity);
      later(() => {
        set(this, 'showTooltip', false);
      }, 2000);

    },

    /**
     * Performs a search task while cancelling the previous one
     * @param {Array} metrics
     */
    onSearch(metrics) {
      const lastSearch = get(this, 'mostRecentSearch');
      if (lastSearch) {
        lastSearch.cancel();
      }
      const task = get(this, 'searchEntities');
      const taskInstance = task.perform(metrics);
      this.set('mostRecentSearch', taskInstance);

      return taskInstance;
    }
  }
});
