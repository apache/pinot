function DataService() {
  this.URL_SEPARATOR = '/';
}

DataService.prototype = {

    // Make synchronous get call
    getDataSynchronous: function(url, data)  {
      var results = undefined;
      $.ajax({
        url: url,
        data: data,
        type: 'get',
        dataType: 'json',
        async: false,
        success: function(data) {
          results = data;
        },
        error: function(e) {
        }
      });
      return results;
    },

    /**
     * Makes an asynchronous GET request to the back end,
     * generates a spinner in the UI
     * execute a callback function on the result of the request,
     * @param  {string}   url      url of the request
     * @param  {string}   data     query string data
     * @param  {Function} callback function to be called after the request
     * @param  {string}   spinArea id of the spinner element
     * @return {[type]}            [description]
     */
    getDataAsynchronous: function(url, data, callback, spinArea)  {
      const target = document.getElementById(spinArea);
      const spinner = new Spinner().spin(target);
      let results;

      return $.ajax({
        url: url,
        data: data,
        type: 'get',
        dataType: 'json',
        success: function(data) {
          results = data;
        },
        error: function(e) {
        }
      }).done(function(data){
        if (callback) {
          callback(data);
        } else {
          return data;
        }
      }).always(function(data) {
        spinner.stop();
      });
    },

    // Make post call
    postData: function(url, data) {
      return $.ajax({
        url: url,
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        type: 'post',
        dataType: 'json',
        data: data
      });
    },
    // FIXME: Tried to put spinner in ajax call, so that each page wont have to handle it separately, as long as 'spin-area' div is placed
    // However, spinner doesn't work if generic 'spin-area' div is placed on all pages (usually doesn't load on anomaly results page when landing on it 2nd time)
    // Hence I've put on each page, a different div id, and that id is passed to the ajax call
    // TODO: Either figure out why generic div id won't work (because passing div id to the ajax caller isn't the best in terms of role separation)
    // Or handle spinner on each page separately (I don't prefer this approach because duplication of logic everywhere)
    fetchMetricSummary: function(dashboard, timeRange, callback) {
      var url = constants.METRIC_SUMMARY;
      var data = {
          dashboard : dashboard,
          timeRange : timeRange
      };
      this.getDataAsynchronous(url, data, callback, 'summary-spin-area');
    },
    fetchAnomalySummary: function(dashboard, timeRanges, callback) {
      var url = constants.ANOMALY_SUMMARY;
      var data = {
          dashboard : dashboard,
          timeRanges : timeRanges.join()
      };
      this.getDataAsynchronous(url, data, callback, 'summary-spin-area');
    },
    fetchWowSummary: function(dashboard, timeRanges, callback) {
      var url = constants.WOW_SUMMARY;
      var data = {
          dashboard : dashboard,
          timeRanges : timeRanges.join()
      };
      this.getDataAsynchronous(url, data, callback, 'summary-spin-area');
    },


    /**
     * Wrapper for all back end search requests
     * @param  {Object} args arguments needed to perform the search
     * @return {Object}      result payload
     */
    fetchAnomalies(args = {}) {
      const {
        anomaliesSearchMode,
        startDate,
        endDate,
        pageNumber,
        metricIds,
        dashboardId,
        anomalyIds,
        anomalyGroupIds,
        functionName,
        updateModelAndNotifyView,
        filterOnly,
        spinner
      } = args;

      switch(anomaliesSearchMode) {
        case constants.MODE_TIME:
          return this.fetchAnomaliesForTime(startDate, endDate, pageNumber, filterOnly, updateModelAndNotifyView, spinner);
        case constants.MODE_METRIC:
          if (!metricIds || !metricIds.length) return;
          return this.fetchAnomaliesForMetricIds(startDate, endDate, pageNumber, metricIds, functionName, filterOnly, updateModelAndNotifyView, spinner);
        case constants.MODE_DASHBOARD:
          if (!dashboardId || !dashboardId.length) return;
          return this.fetchAnomaliesForDashboardId(startDate, endDate, pageNumber, dashboardId, functionName, filterOnly, updateModelAndNotifyView, spinner);
        case constants.MODE_ID:
          if (!anomalyIds || !anomalyIds.length) return;
          return this.fetchAnomaliesForAnomalyIds(startDate, endDate, pageNumber, anomalyIds, functionName, filterOnly, updateModelAndNotifyView, spinner);
        case constants.MODE_GROUPID:
          if (!anomalyGroupIds || !anomalyGroupIds.length) return;
          return this.fetchAnomaliesforGroupIds(startDate, endDate, pageNumber, anomalyGroupIds, functionName, filterOnly, updateModelAndNotifyView, spinner);
        default:
          return;
      }
    },
    // Fetch anomalies for metric ids in array in time range
    fetchAnomaliesForMetricIds : function(startTime, endTime, pageNumber, metricIds, functionName, filterOnly, callback, spinner) {
      const url = constants.SEARCH_ANOMALIES_METRICIDS + startTime + this.URL_SEPARATOR + endTime + this.URL_SEPARATOR + pageNumber;
      const data = {
        metricIds: metricIds,
        functionName,
        filterOnly
      };
      return this.getDataAsynchronous(url, data, callback, 'anomaly-spin-area');
    },
    // Fetch anomalies for dashboard id in time range
    fetchAnomaliesForDashboardId : function(startTime, endTime, pageNumber, dashboardId, functionName, filterOnly, callback, spinner) {
      const url = constants.SEARCH_ANOMALIES_DASHBOARDID + startTime + this.URL_SEPARATOR + endTime + this.URL_SEPARATOR + pageNumber;
      const data = {
        dashboardId,
        functionName,
        filterOnly
      };
      return this.getDataAsynchronous(url, data, callback, 'anomaly-spin-area');
    },
    // Fetch anomalies for anomaly ids in array in time range
    fetchAnomaliesForAnomalyIds : function(startTime, endTime, pageNumber, anomalyIds, functionName, filterOnly, callback, spinner) {
      const url = constants.SEARCH_ANOMALIES_ANOMALYIDS + startTime + this.URL_SEPARATOR + endTime + this.URL_SEPARATOR + pageNumber;
      const data = {
        anomalyIds,
        functionName,
        filterOnly
      };
      return this.getDataAsynchronous(url, data, callback, spinner);
    },

    // Fetch anomalies for group ids in array
    fetchAnomaliesforGroupIds(startTime, endTime, pageNumber, anomalyGroupIds, functionName, filterOnly, callback, spinner) {
      const url = constants.SEARCH_ANOMALIES_GROUPIDS + startTime + this.URL_SEPARATOR + endTime + this.URL_SEPARATOR + pageNumber;
      var data = {
        anomalyGroupIds,
        functionName,
        filterOnly
      };
      this.getDataAsynchronous(url, data, callback, 'anomaly-spin-area');
    },

    // Fetch anomalies for anomaly ids in array in time range
    fetchAnomaliesForTime : function(startTime, endTime, pageNumber, filterOnly, callback, spinner) {
      const url = constants.SEARCH_ANOMALIES_TIME + startTime + this.URL_SEPARATOR + endTime + this.URL_SEPARATOR + pageNumber;
      const data = {
        filterOnly
      };

      return this.getDataAsynchronous(url, data, callback, spinner);
    },
    // Update anomaly feedback for anomaly id
    updateFeedback : function(anomalyId, feedbackType, comment) {
      var url = constants.UPDATE_ANOMALY_FEEDBACK + anomalyId;
      const data = {
        feedbackType,
        comment
      };
      var response = this.postData(url, JSON.stringify(data));
    },

    fetchGranularityForMetric(metricId) {
      const url = constants.METRIC_GRANULARITY + metricId;
      return this.getDataAsynchronous(url);
    },

    fetchDimensionsForMetric(metricId) {
      const url = constants.METRIC_DIMENSION + metricId;
      return this.getDataAsynchronous(url);
    },

    fetchFiltersForMetric(metricId) {
      const url = constants.METRIC_FILTERS + metricId;
      return this.getDataAsynchronous(url);
    },

    fetchMaxTimeForMetric(metricId) {
      const url = constants.METRIC_MAX_TIME + metricId;
      return this.getDataAsynchronous(url);
    },

    fetchTimeseriesCompare: function (metricId, currentStart, currentEnd, baselineStart, baselineEnd,
        dimension = "ALL", filters = {}, granularity = "DAYS") {
      var url = "/timeseries/compare/" + metricId + "/" + currentStart + "/" + currentEnd + "/"
          + baselineStart + "/" + baselineEnd + "?dimension=" + dimension + "&filters="
          + encodeURIComponent(JSON.stringify(filters)) + "&granularity=" + granularity;
      return this.getDataAsynchronous(url, {}, null, 'analysis-graph-spin-area');
    },

  fetchHeatmapData: function (metricId, currentStart, currentEnd, baselineStart, baselineEnd,
      filters = {}) {
    var url = "/data/heatmap/" + metricId + "/" + currentStart + "/" + currentEnd + "/"
        + baselineStart + "/" + baselineEnd + "?filters=" + encodeURIComponent(JSON.stringify(filters));
    return this.getDataAsynchronous(url, {}, null, 'dimension-tree-spin-area');
  },

  fetchAnomalyWowData(anomalyId){
    const url = `/anomalies/${anomalyId}`;
    return this.getDataSynchronous(url);
  },

  fetchMetricByMetricId(metricId) {
    const url = `/data/metric/${metricId}`;
    return this.getDataSynchronous(url);
  },

  fetchRootCauseData(currentStart, baselineStart, windowSize, inputUrn) {
    const url = `/rootcause/query?framework=rootCause&current=${currentStart}&baseline=${baselineStart}&windowSize=${windowSize}&urns=${inputUrn}`;
    return this.getDataAsynchronous(url, {}, null, 'rootcause-table-spin-area');
  }
};
