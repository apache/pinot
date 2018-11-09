/*global html2canvas, jsPDF*/

/**
 * Handles metrics import from inGrahps dashboards
 * @module self-serve/create/import-metric
 * @exports import-metric
 */
import Controller from '@ember/controller';
import $ from 'jquery';
import _ from 'lodash';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import { isBlank } from '@ember/utils';
import { task } from 'ember-concurrency';
import { inject as service } from '@ember/service';
import {
  get,
  set,
  setProperties,
  computed
} from '@ember/object';
import { appendFilters } from 'thirdeye-frontend/utils/rca-utils';
import { humanizeFloat, checkStatus } from 'thirdeye-frontend/utils/utils';

const CUSTOMIZE_OPTIONS = [{
  id: 0,
  label: 'Nothing',
  value: 'Nothing'
}, {
  id: 1,
  label: 'WoW',
  value: 'Wow'
}, {
  id: 2,
  label: 'Wo2W',
  value: 'Wo2w'
}, {
  id: 3,
  label: 'Median4W',
  value: 'Median4w'
}];

export default Controller.extend({
  shareDashboardApiService: service('services/api/share-dashboard'),
  anomalyResponseFilterTypes: _.cloneDeep(anomalyUtil.anomalyResponseObj),
  showCopyTooltip: false,
  showSharedTooltip: false,
  shareUrl: null,
  showWow: false,
  showWo2w: false,
  showMedian4w: false,
  showDashboardSummary: false,
  offsetsMap: {},
  options: [],
  options_two: [],
  colspanNum: 4,

  init() {
    this._super(...arguments);
    // Add ALL option to copy of global anomaly response object
    const anomalyResponseFilterTypes = _.cloneDeep(anomalyUtil.anomalyResponseObj);
    anomalyResponseFilterTypes.push({
      name: 'All Resolutions',
      value: 'ALL',
      status: 'All Resolutions'
    });
    set(this, 'anomalyResponseFilterTypes', anomalyResponseFilterTypes);

    // Add the options for compare
    set(this, 'options_two', set(this, 'options', CUSTOMIZE_OPTIONS));
  },

  /**
   * @summary Returns the
   * @return {Array}
   */
  _getAnomalyByOffsetsMapping: task (function * () {
    const applicationAnomalies = get(this, 'model.anomalyMapping');
    const offsetsMapping = yield get(this, '_fetchOffsets').perform(applicationAnomalies);
    return offsetsMapping;
  }).drop(),

  /**
   * @summary Returns the config object for the custom share template header.
   * @return {object} The object of metrics, with array of alerts and array anomalies
   * @example

   */
  shareConfig: computed(
    'shareTemplateConfig',
    function() {
      let shareTemplateConfig = get(this, 'shareTemplateConfig');
      if(_.isEmpty(shareTemplateConfig)) {
        shareTemplateConfig = false;
      }
      return shareTemplateConfig;
    }
  ),

  /**
   * @summary Returns the anomalies filtered from the filter options on the dashboard
   * @return {object} The object of metrics, with array of alerts and array anomalies
   * @example
    { MetricNameA: [
        alertNameA1: [{anomaly1}, {anomaly2}, {anomalyN}]
      ],
      MetricNameN: [...]
    }
   */
  filteredAnomalyMapping: computed(
    'model.{anomalyMapping,feedbackType}',
    function() {
      let anomalyMapping = get(this, 'model.anomalyMapping');
      const feedbackType = get(this, 'model.feedbackType');
      const feedbackItem = this._checkFeedback(feedbackType);

      if (feedbackItem.value !== 'ALL' && !isBlank(anomalyMapping)) {
        let map = {};
        let index = 1;
        // Iterate through each anomaly
        Object.keys(anomalyMapping).some(function(metric) {
          Object.keys(anomalyMapping[metric].items).some(function(alert) {
            anomalyMapping[metric].items[alert].items.forEach(item => {
              if (item.anomaly.data.feedback === feedbackItem.value) {
                const metricName = get(item.anomaly, 'metricName');
                const metricId = get(item.anomaly, 'metricId');
                const functionName = get(item.anomaly, 'functionName');
                const functionId = get(item.anomaly, 'functionId');

                if (!map[metricName]) {
                  map[metricName] = { 'metricId': metricId, items: {}, count: index };
                  index++;
                }

                if(!map[metricName].items[functionName]) {
                  map[metricName].items[functionName] = { 'functionId': functionId, items: [] };
                }

                map[metricName].items[functionName].items.push(item);
              }
            });
          });

        });
        return map;
      } else {
        return anomalyMapping;
      }

    }
  ),

  anomaliesFilteredCount: computed(
    'filteredAnomalyMapping',
    function() {
      const filteredAnomalyMapping = get(this, 'filteredAnomalyMapping');
      let count = 0;

      Object.keys(filteredAnomalyMapping).some(function(metric) {//key = metric
        if (filteredAnomalyMapping[metric] && filteredAnomalyMapping[metric].items) {
          Object.keys(filteredAnomalyMapping[metric].items).some(function(alert) {//item = alert
            count += filteredAnomalyMapping[metric].items[alert].items.length;
          });
        }
      });

      return count;
    }
  ),

  tree: computed(
    'filteredAnomalyMapping', 'model.shareMetaData',
    function() {
      const shareMetaData = get(this, 'model.shareMetaData');
      const filteredAnomalyMapping = get(this, 'filteredAnomalyMapping');
      let viewTree, viewTreeFirstChild = [];
      if (shareMetaData && shareMetaData.meta) {
        viewTree = [shareMetaData.meta.blob];
        //update the filteredAnomalyMapping with references to its associated treeNode
        viewTreeFirstChild = viewTree.get('firstObject').children;
        Object.keys(filteredAnomalyMapping).some((metric) => {
          Object.keys(filteredAnomalyMapping[metric].items).some((alert) => {
            // Keeping a reference to this new tree node for this alert in filteredAnomalyMapping
            if (typeof metric === 'string') {
              const metricId = filteredAnomalyMapping[metric].metricId;
              const functionId = filteredAnomalyMapping[metric].items[alert].functionId;
              filteredAnomalyMapping[metric].items[alert].viewTreeNode = viewTreeFirstChild.find(metric => metric.id === metricId).children.find(alert => alert.id === functionId);
            }
          });
          // Keeping a reference to this new tree node for this metric in filteredAnomalyMapping
          if (typeof metric === 'string') {
            const metricId = filteredAnomalyMapping[metric].metricId;
            filteredAnomalyMapping[metric].viewTreeNode = viewTreeFirstChild.find(metric => metric.id === metricId);
          }
        });
        return viewTree;
      }

      viewTree = [{
        id: 0,
        name: 'Customize metrics/alerts',
        type: 'root',
        isExpanded: false,
        isSelected: false,
        isVisible: true,
        isChecked: true,
        comment: null,
        children: []
      }];

      viewTreeFirstChild = viewTree.get('firstObject').children;
      Object.keys(filteredAnomalyMapping).some((metric, index) => {
        let tempChildren = [];
        if (filteredAnomalyMapping[metric] && filteredAnomalyMapping[metric].items) {
          Object.keys(filteredAnomalyMapping[metric].items).some((alert, index) => {
            tempChildren.push({
              id: filteredAnomalyMapping[metric].items[alert].functionId,
              name: alert,
              type: 'alert',
              isExpanded: true,
              isSelected: false,
              isVisible: true,
              isChecked: true,
              isComment: false,
              comment: null,
              children: []
            });
            // Keeping a reference to this new tree node for this alert in filteredAnomalyMapping
            filteredAnomalyMapping[metric].items[alert].viewTreeNode = tempChildren[index];
          });

          //add each metric to tree selection
          viewTreeFirstChild.push({
            id: filteredAnomalyMapping[metric].metricId,
            name: metric,
            type: 'metric',
            isExpanded: true,
            isSelected: false,
            isVisible: true,
            isChecked: true,
            isComment: true,
            comment: null,
            children: tempChildren
          });
          // Keeping a reference to this new tree node for this metric in filteredAnomalyMapping
          filteredAnomalyMapping[metric].viewTreeNode = viewTreeFirstChild[index];
        }
      });

      //save the new tree view values
      return viewTree;
    }
  ),

  dashboard_summary_comment: computed(
    'tree.@each.comment',
    function() {
      return get(this, 'tree')[0].comment;
    }),

  /**
   * Helper for getting the matching selected response feedback object
   * @param {string} selected - selected filter by value
   * @return {string}
   */
  _checkFeedback: function(selected) {
    return get(this, 'anomalyResponseFilterTypes').find((type) => {
      return type.name === selected;
    });
  },

  _fetchOffsets: task (function * (anomalyMapping) {
    if (!anomalyMapping) { return; }

    let map = {};
    let index = 1;
    // Iterate through each anomaly
    yield Object.keys(anomalyMapping).some(function(metric) {
      Object.keys(anomalyMapping[metric].items).some(function(alert) {
        anomalyMapping[metric].items[alert].items.forEach(async (item) => {
          const metricName = get(item.anomaly, 'metricName');
          const metricId = get(item.anomaly, 'metricId');
          const functionName = get(item.anomaly, 'functionName');
          const functionId = get(item.anomaly, 'functionId');

          const dimensions = get(item.anomaly, 'dimensions');
          const start = get(item.anomaly, 'start');
          const end = get(item.anomaly, 'end');

          if (!map[metricName]) {
            map[metricName] = { 'metricId': metricId, items: {}, count: index };
            index++;
          }

          if(!map[metricName].items[functionName]) {
            map[metricName].items[functionName] = { 'functionId': functionId, items: [] };
          }

          const filteredDimensions = Object.keys(dimensions).map(key => [key, '=', dimensions[key]]);
          //build new urn
          const metricUrn = appendFilters(`thirdeye:metric:${metricId}`, filteredDimensions);
          //Get all in the following order - current,wo2w,median4w
          const offsets = await fetch(`/rootcause/metric/aggregate/batch?urn=${metricUrn}&start=${start}&end=${end}&offsets=wo1w,wo2w,median4w&timezone=America/Los_Angeles`).then(checkStatus).then(res => res);

          set(item.anomaly, 'offsets',  offsets ? {
            'wow': humanizeFloat(offsets[0]),
            'wo2w': humanizeFloat(offsets[1]),
            'median4w': humanizeFloat(offsets[2])
          } : {
            'wow': '-',
            'wo2w': '-',
            'median4w': '-'
          });

          map[metricName].items[functionName].items.push(item);
        });
      });
    });
    // return updated anomalyMapping
    return anomalyMapping;
  }).drop(),

  /**
   * Helper for getting the matching selected response feedback object
   * @param {string} text - this could be anything that we want to do a copy to clipboard
   * @return {string} - the copied string
   */
  _copyFromDummyInput: function(text) {
    // Create a dummy input to copy the string array inside it
    var dummy = document.createElement('input');

    // Add it to the document
    document.body.appendChild(dummy);

    // Set its ID
    dummy.setAttribute('id', 'dummy_id');

    // Output the array into it
    dummy.value=text;

    // Select it
    dummy.select();

    // Copy its contents
    document.execCommand('copy');

    // Remove it as its not needed anymore
    document.body.removeChild(dummy);

    return text;
  },

  async _getOffsets() {
      const map = get(this, 'offsetsMap');
      if (Object.keys(map).length === 0 ) {//Only fetch if empty
        const result = await get(this, '_getAnomalyByOffsetsMapping').perform();
        let index = 1;
        // Iterate through each anomaly
        Object.keys(result).some(function(metric) {
          Object.keys(result[metric].items).some(function(alert) {
            result[metric].items[alert].items.forEach(item => {
                const metricName = get(item.anomaly, 'metricName');
                const metricId = get(item.anomaly, 'metricId');
                const functionName = get(item.anomaly, 'functionName');
                const functionId = get(item.anomaly, 'functionId');

                if (!map[metricName]) {
                  map[metricName] = { 'metricId': metricId, items: {}, count: index };
                  index++;
                }

                if(!map[metricName].items[functionName]) {
                  map[metricName].items[functionName] = { 'functionId': functionId, items: [] };
                }

                map[metricName].items[functionName].items.push(item);
            });
          });
        });
        setProperties(this, {
          'model.anomalyMapping': map,
          'offsetsMap': map
        });
      }

      // //show median4w column
      // this.toggleProperty('showMedian4w');
  },

  _customizeEmailHelper(option, type) {
    //reset both selects' options
    set(this, 'options_two', set(this, 'options', CUSTOMIZE_OPTIONS));
    //hide all except if the sibling's value and not `nothing`
    setProperties(this, {
      'showWow': false,
      'showWo2w': false,
      'showMedian4w': false
    });

    //Show sibling
    const customizeEmail1 = document.getElementById('customizeEmail1').value;
    const customizeEmail2 = document.getElementById('customizeEmail2').value;

    const sibingValue = type === 'one' ? customizeEmail2 : customizeEmail1;
    this.toggleProperty(`show${sibingValue}`);

    //Calculates colspan Number
    const showCustomizeEmailTemplate = get(this, 'showCustomizeEmailTemplate');
    if(showCustomizeEmailTemplate && (customizeEmail1 === 'Nothing' || customizeEmail2 === 'Nothing')) {
      set(this, 'colspanNum', 5);
    } else if (showCustomizeEmailTemplate) {
      set(this, 'colspanNum', 6);
    } else {
      set(this, 'colspanNum', 4);
    }

    //limited sibling list to selected choice
    const limitedSiblingOptions = CUSTOMIZE_OPTIONS.filter(function(item){
      return item.value !== option;
    });
    //limited current list to sibling's existing selected
    const limitedSelfOptions = CUSTOMIZE_OPTIONS.filter(function(item){
      return item.value !== sibingValue;
    });

    if (type === 'one') {
      set(this, 'options_two', limitedSiblingOptions);
      set(this, 'options', limitedSelfOptions);
    } else {
      set(this, 'options', limitedSiblingOptions);
      set(this, 'options_two', limitedSelfOptions);
    }

    // fetch base on offset
    if (option !== 'Nothing') {
      this._getOffsets();
    }

    switch(option) {
      case 'Nothing':
        //hide accordingly
        break;
      case 'Wow':
        //show wow column and it's sibling
        this.toggleProperty('showWow');
        break;
      case 'Wo2w':
        //show wo2w column and it's sibling
        this.toggleProperty('showWo2w');
        break;
      case 'Median4w':
        //show median4w column and it's sibling
        this.toggleProperty('showMedian4w');
        break;
    }
  },

  actions: {
    onCustomizeEmail1(option) {
      this._customizeEmailHelper(option, 'one');
    },

    onCustomizeEmail2(option) {
      this._customizeEmailHelper(option, 'two');
    },

    /**
     * Created a PDF to save
     */
    createPDF() {
      $('.share-dashboard-container__preview-container-body').css({backgroundColor: '#EDF0F3'});
      html2canvas($('.share-dashboard-container__preview-container-body'), {
        onrendered: (canvas) => {
          const imgData = canvas.toDataURL('image/jpeg', 1.0);
          let doc;
          if(canvas.width > canvas.height){
            doc = new jsPDF('l', 'pt', [canvas.width, canvas.height]);
          }
          else{
            doc = new jsPDF('p', 'pt', [canvas.height, canvas.width]);
          }
          doc.addImage(imgData, 'PNG', 10, 10);
          doc.save(`shared_summary.pdf`);
          $('.share-dashboard-container__preview-container-body').css({backgroundColor: 'transparent'});
        }
      });
    },

    /**
     * @summary to copy and paste the share summary to clipboard
     */
    copy() {
      //disable all unneeded doms for copy
      $('.te-nav').css({display: 'none'});
      $('.share-dashboard-container__share-configure').css({display: 'none'});
      $('.share-dashboard-container > header').css({display: 'none'});
      $('.share-dashboard-container__share-body').css({display: 'none'});
      //invoke selectAll and copy commands
      document.execCommand('selectAll');
      document.execCommand('copy');
      document.execCommand('unselect');
      $('.te-nav').css({display: 'block'});
      $('.share-dashboard-container__share-configure').css({display: 'block'});
      $('.share-dashboard-container > header').css({display: 'block'});
      $('.share-dashboard-container__share-body').css({display: 'block'});
      set(this, 'showCopyTooltip', true);
    },

    /**
     * @summary Update the comment on the shared view and update the metadata tree with latest comments to be saved later on `get share link` action.
     * @param {string} id - the id associated with the checkbox element
     */
    updateComment(id) {
      const userComment = document.getElementById(id).value;
      if (id === 'dashboard_summary') {
        //update the tree with latest comment
        let res = get(this, 'tree.firstObject');
        set(res, 'comment', userComment);
      } else {
        //update the tree with latest comment
        let res = get(this, 'tree.firstObject.children').filter(metric => metric.id === id);
        set(res, 'firstObject.comment', userComment);
      }

    },

    /**
     * @summary Update the hide/show of the on the shared view and update the metadata tree with latest values to be saved later on `get share link` action.
     * @param {string} id - the id associated with the checkbox element
     */
    toggleSelection(id) {
      const element = document.getElementById(`section_${id}`);
      if (element) {
        element.style.display = element.style.display === 'none' ? 'block' : 'none';
        if (id === 'dashboard_summary') {
          //update the tree with latest checkbox
          let res = get(this, 'tree.firstObject');
          res.display = element.style.display;
        } else {
          //update the tree with latest checkbox
          let res = get(this, 'tree.firstObject.children').filter(metric => metric.id === id);
          get(res, 'firstObject').display = element.style.display;
        }
      }

      if (id === 'dashboard_summary') {
        this.toggleProperty('showDashboardSummary');
      } else if (id === 'customize_email') {
        this.toggleProperty('showCustomizeEmailTemplate');
      }
    },

    /**
     * @summary Save the latest metadata and retrieve a sharable link.
     * @param {string} id - the id associated with the checkbox element
     */
    getShareLink() {
      const treeView = get(this, 'tree').get('firstObject');
      const shareResponse = get(this, 'shareDashboardApiService').saveShareDashboard(treeView);
      const hashKey = get(this, 'shareDashboardApiService').getHashKey();
      //ADD to helper method
      let currentUrl = `${window.location.origin}/app/#/home/share-dashboard?`;
      if(get(this, 'model.appName')){
        currentUrl = currentUrl.concat(`appName=${get(this, 'model.appName')}`);
      }
      if(get(this, 'model.startDate')){
        currentUrl = currentUrl.concat(`&startDate=${get(this, 'model.startDate')}`);
      }
      if(get(this, 'model.endDate')){
        currentUrl = currentUrl.concat(`&endDate=${get(this, 'model.endDate')}`);
      }
      if(get(this, 'model.duration')){
        currentUrl = currentUrl.concat(`&duration=${get(this, 'model.duration')}`);
      }
      if(get(this, 'model.feedbackType')){
        currentUrl = currentUrl.concat(`&feedbackType=${get(this, 'model.feedbackType')}`);
      }
      currentUrl = currentUrl.concat(`&shareId=${hashKey}`);
      shareResponse.then(() => {
        set(this, 'showSharedTooltip', true);
        set(this, 'shareUrl', currentUrl);
        //update the route's params
        const { appName, duration, startDate, endDate } = get(this, 'model');
        this.transitionToRoute({ queryParams: { appName, duration, startDate, endDate, shareId: hashKey }});
      });
      //has to be here to tie user event with the copy action (vs in the promise return above)
      this._copyFromDummyInput(currentUrl);
      return false;
    }

  }
});
