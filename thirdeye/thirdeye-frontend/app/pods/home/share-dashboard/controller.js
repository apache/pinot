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
import { inject as service } from '@ember/service';
import {
  get,
  set,
  setProperties,
  computed
} from '@ember/object';
import { reads } from '@ember/object/computed';

const CUSTOMIZE_OPTIONS = [{
  id: 0,
  label: 'Nothing',
  value: 'Nothing',
  selected: null
}, {
  id: 1,
  label: 'WoW',
  value: 'Wow',
  selected: null
}, {
  id: 2,
  label: 'Wo2W',
  value: 'Wo2w',
  selected: null
}, {
  id: 3,
  label: 'Median4W',
  value: 'Median4w',
  selected: null
}];

export default Controller.extend({
  shareDashboardApiService: service('services/api/share-dashboard'),
  anomalyResponseFilterTypes: _.cloneDeep(anomalyUtil.anomalyResponseObj),
  showCopyTooltip: false,
  showSharedTooltip: false,
  shareUrl: null,
  showDashboardSummary: reads('tree.firstObject.showDashboardSummary'),
  showCustomizeEmailTemplate: reads('tree.firstObject.showCustomizeEmailTemplate'),
  showWow: reads('tree.firstObject.showWow'),
  showWo2w: reads('tree.firstObject.showWo2w'),
  showMedian4w: reads('tree.firstObject.showMedian4w'),
  options: reads('tree.firstObject.options'),
  options_two: reads('tree.firstObject.options_two'),
  colspanNum: reads('tree.firstObject.colspanNum'),

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
  },

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
              set(filteredAnomalyMapping[metric].items[alert], 'viewTreeNode', viewTreeFirstChild.find(metric => metric.id === metricId).children.find(alert => alert.id === functionId));
            }
          });
          // Keeping a reference to this new tree node for this metric in filteredAnomalyMapping
          if (typeof metric === 'string') {
            const metricId = filteredAnomalyMapping[metric].metricId;
            set(filteredAnomalyMapping[metric], 'viewTreeNode', viewTreeFirstChild.find(metric => metric.id === metricId));
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
        children: [],
        showWow: false,
        showWo2w: false,
        showMedian4w: false,
        showDashboardSummary: false,
        showCustomizeEmailTemplate: false,
        options: [ ...CUSTOMIZE_OPTIONS],
        options_two: [ ...CUSTOMIZE_OPTIONS],
        colspan: 4
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
            set(filteredAnomalyMapping[metric].items[alert], 'viewTreeNode', tempChildren[index]);
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
          set(filteredAnomalyMapping[metric], 'viewTreeNode', viewTreeFirstChild[index]);
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


  /**
   * Helper for toggling show properties in tree.firstObject
   * @param {string} property - showWow, showWo2w, showMedian4w, showDashboardSummary, or showCustomizeEmailTemplate
   * @return {none} - this helper is just a setter
   */
  _toggleTreeProperty(property) {
    let currentState = get(this, 'tree.firstObject');
    set(currentState, property, !currentState[property]);
  },

  /**
   * Helper for setting selected in email selectors
   * @param {string} selectedBaseline - Wow, Wo2w, Median4w
   * @param {Array} choices - [{}, {}, ...]
   * @return {none} - this helper is just a setter
   */
  _selectOption(choices, selectedBaseline) {
    const newChoices = [];
    choices.forEach(choice => {
      const newObj = { ...choice};
      if (newObj.value === selectedBaseline) {
        newObj.selected = 'selected';
      } else {
        newObj.selected = null;
      }
      newChoices.push(newObj);
    });
    return newChoices;
  },

  _customizeEmailHelper(option, type) {
    //reset both selects' options
    set(this, 'tree.firstObject.options', [ ...CUSTOMIZE_OPTIONS]);
    set(this, 'tree.firstObject.options_two', [ ...CUSTOMIZE_OPTIONS]);
    //hide all except if the sibling's value and not `nothing`
    let currentState = get(this, 'tree.firstObject');
    setProperties(currentState, {
      'showWow': false,
      'showWo2w': false,
      'showMedian4w': false
    });

    //Show sibling
    const customizeEmail1 = document.getElementById('customizeEmail1').value;
    const customizeEmail2 = document.getElementById('customizeEmail2').value;

    const siblingValue = type === 'one' ? customizeEmail2 : customizeEmail1;
    this._toggleTreeProperty(`show${siblingValue}`);

    //Calculates colspan Number
    const showCustomizeEmailTemplate = get(this, 'showCustomizeEmailTemplate');
    if(showCustomizeEmailTemplate && (customizeEmail1 === 'Nothing' || customizeEmail2 === 'Nothing')) {
      set(this, 'tree.firstObject.colspanNum', 5);
    } else if (showCustomizeEmailTemplate) {
      set(this, 'tree.firstObject.colspanNum', 6);
    } else {
      set(this, 'tree.firstObject.colspanNum', 4);
    }

    let limitedSelfOptions = this._selectOption([ ...CUSTOMIZE_OPTIONS], option);
    let limitedSiblingOptions = this._selectOption([ ...CUSTOMIZE_OPTIONS], siblingValue);

    //limited sibling list to selected choice
    limitedSiblingOptions = limitedSiblingOptions.filter(function(item){
      return item.value !== option;
    });
    //limited current list to sibling's existing selected
    limitedSelfOptions = limitedSelfOptions.filter(function(item){
      return item.value !== siblingValue;
    });

    switch(option) {
      case 'Nothing':
        //hide accordingly
        break;
      case 'Wow':
        //show wow column and it's sibling
        this._toggleTreeProperty('showWow');
        break;
      case 'Wo2w':
        //show wo2w column and it's sibling
        this._toggleTreeProperty('showWo2w');
        break;
      case 'Median4w':
        //show median4w column and it's sibling
        this._toggleTreeProperty('showMedian4w');
        break;
    }

    if (type === 'one') {
      set(this, 'tree.firstObject.options_two', limitedSiblingOptions);
      set(this, 'tree.firstObject.options', limitedSelfOptions);
    } else {
      set(this, 'tree.firstObject.options', limitedSiblingOptions);
      set(this, 'tree.firstObject.options_two', limitedSelfOptions);
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
        this._toggleTreeProperty('showDashboardSummary');
      } else if (id === 'customize_email') {
        this._toggleTreeProperty('showCustomizeEmailTemplate');
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
      if(get(this, 'model.subGroup')){
        currentUrl = currentUrl.concat(`subGroup=${get(this, 'model.subGroup')}`);
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
        const { appName, subGroup, duration, startDate, endDate } = get(this, 'model');
        this.transitionToRoute({ queryParams: { appName, subGroup, duration, startDate, endDate, shareId: hashKey }});
      });
      //has to be here to tie user event with the copy action (vs in the promise return above)
      this._copyFromDummyInput(currentUrl);
      return false;
    }

  }
});
