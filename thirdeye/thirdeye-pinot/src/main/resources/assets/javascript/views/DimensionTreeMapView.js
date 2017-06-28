function DimensionTreeMapView(dimensionTreeMapModel) {
  const template = $("#dimension-tree-map-template").html();
  const graphTemplate = $("#dimension-tree-map-graph-template").html();
  this.compiledTemplate = Handlebars.compile(template);
  this.compiledGraph = Handlebars.compile(graphTemplate);
  this.placeHolderId = "#dimension-tree-map-placeholder";
  this.graphPlaceholderId = "#dimension-tree-map-graph-placeholder";

  this.dimensionTreeMapModel = dimensionTreeMapModel;
  this.currentRange = () => {
    return {
      'Last 24 Hours': [moment().subtract(1, 'days'), moment()],
      'Yesterday': [moment().subtract(2, 'days'), moment().subtract(1, 'days')],
      'Last 7 Days': [moment().subtract(6, 'days'), moment()],
      'Last 30 Days': [moment().subtract(29, 'days'), moment()],
      'This Month': [moment().startOf('month'), moment().endOf('month')],
      'Last Month': [moment().subtract(1, 'month').startOf('month'),
        moment().subtract(1, 'month').endOf('month')]
    };
  };

  this.baselineRange = () => {
    const range = {};
    constants.COMPARE_MODE_OPTIONS.forEach((options) => {
      const offset = constants.WOW_MAPPING[options];
      range[options] = [this.calculateBaselineDate('currentStart', offset),this.calculateBaselineDate('currentEnd', offset)];
    });
   return range;
  };
}

DimensionTreeMapView.prototype = {
  render() {
    if (this.dimensionTreeMapModel.heatmapData) {
      const dimensionTreeMapModel = this.makeUrl(this.dimensionTreeMapModel);

      var compiledTemplate = this.compiledTemplate(dimensionTreeMapModel);
      $(this.placeHolderId).html(compiledTemplate);
      this.renderTreemapHeaderSection();
      this.renderTreemapSection();
      this.setupListenersForMode();
    }
  },

  /**
   * Takes propertie from dimensionTreeMapModel
   * and creates an url to the new ember app
   * @param {Object} dimensionTreeMapModel
   */
  makeUrl(dimensionTreeMapModel) {
    let {
      granularity,
      currentStart,
      currentEnd,
      heatMapFilters,
      compareMode,
      metricId
    } = dimensionTreeMapModel;

    // needed because of inconsistency (5_MINUTES = MINUTES)
    granularity = (granularity === '5_MINUTES')
      ? 'MINUTES'
      : granularity;

    const offset = {
      MINUTES: 6,
      HOURS: 72,
      DAYS: 168
    }[granularity] || 0;

    const startDate = currentStart.clone().subtract(offset, 'hours');
    const endDate = currentEnd.clone().add(offset, 'hours');

    const url = `app/#/rca/metrics/${metricId}?startDate=${startDate.valueOf()}&endDate=${endDate.valueOf()}&granularity=${granularity}&compareMode=${compareMode}&filters=${JSON.stringify(heatMapFilters)}`

    return Object.assign({}, dimensionTreeMapModel, { rcaMetricUrl: url })
  },

  destroy() {
    const $heatMapCurrent = $('#heatmap-current-range');
    const $heatMapBaseline = $('#heatmap-baseline-range');

    $heatMapCurrent.length && $heatMapCurrent.data('daterangepicker').remove();
    $heatMapBaseline.length && $heatMapBaseline.data('daterangepicker').remove();
    $("#dimension-tree-map-placeholder").children().remove();
  },

  renderTreemapHeaderSection : function() {
    const $currentRangeText = $('#heatmap-current-range span');
    const $baselineRangeText = $('#heatmap-baseline-range span');
    const $currentRange = $('#heatmap-current-range');
    const $baselineRange = $('#heatmap-baseline-range');
    const showTime = this.dimensionTreeMapModel.granularity !== constants.GRANULARITY_DAY;
    const dateFormat = showTime ? constants.DATE_TIME_RANGE_FORMAT : constants.DATE_RANGE_FORMAT;

    const setRangeText = (selector, start, end, dateFormat, compareMode) => {
       selector.addClass("time-range").html(
        `<span class="time-range__type">${compareMode}</span> ${start.format(dateFormat)} &mdash; ${end.format(dateFormat)}`);
    };

    const baselineCallBack = (start, end, compareMode = constants.DATE_RANGE_CUSTOM) => {
      // show time needs to be after
      const showTime = this.dimensionTreeMapModel.granularity !== constants.GRANULARITY_DAY;
      const dateFormat = showTime ? constants.DATE_TIME_RANGE_FORMAT : constants.DATE_RANGE_FORMAT;
      this.dimensionTreeMapModel['baselineStart'] = start;
      this.dimensionTreeMapModel['baselineEnd'] = end;
      this.dimensionTreeMapModel['compareMode'] = compareMode;
      setRangeText($baselineRangeText, start, end, dateFormat, compareMode);
      this.destroyTreemapSection();
      this.dimensionTreeMapModel.update().then(() => {
        this.renderTreemapSection();
      });
    };

    const currentCallBack = (start, end, rangeType = constants.DATE_RANGE_CUSTOM) => {
      const showTime = this.dimensionTreeMapModel.granularity !== constants.GRANULARITY_DAY;
      const dateFormat = showTime ? constants.DATE_TIME_RANGE_FORMAT : constants.DATE_RANGE_FORMAT;
      const $baselineRangePicker = $('#heatmap-baseline-range');
      const baselineCompareMode = this.dimensionTreeMapModel['compareMode'];
      const compareMode = this.dimensionTreeMapModel['compareMode'];

      this.dimensionTreeMapModel['currentStart'] = start;
      this.dimensionTreeMapModel['currentEnd'] = end;

      if (baselineCompareMode !== constants.DATE_RANGE_CUSTOM) {
        const offset = constants.WOW_MAPPING[compareMode];
        const baselineStart = start.clone().subtract(offset, 'days');
        const baselineEnd = end.clone().subtract(offset, 'days');
        $baselineRangePicker.length && $baselineRangePicker.data('daterangepicker').remove();
        this.renderDatePicker($baselineRange, baselineCallBack, baselineStart, baselineEnd, showTime, this.baselineRange, 'left');
        baselineCallBack(baselineStart, baselineEnd, baselineCompareMode);
      }
      setRangeText($currentRangeText, start, end, dateFormat, rangeType);
    };

        // TIME RANGE SELECTION
    const currentStart = this.dimensionTreeMapModel.currentStart;
    const currentEnd = this.dimensionTreeMapModel.currentEnd;
    const baselineStart = this.dimensionTreeMapModel.baselineStart;
    const baselineEnd = this.dimensionTreeMapModel.baselineEnd;


    this.renderDatePicker($currentRange, currentCallBack, currentStart, currentEnd, showTime, this.currentRange);
    this.renderDatePicker($baselineRange, baselineCallBack, baselineStart, baselineEnd, showTime, this.baselineRange, 'left');

    setRangeText($currentRangeText, currentStart, currentEnd, dateFormat, constants.DATE_RANGE_CUSTOM);
    setRangeText($baselineRangeText, baselineStart, baselineEnd, dateFormat, this.dimensionTreeMapModel['compareMode']);
  },

  renderDatePicker: function (domId, callbackFun, initialStart, initialEnd, showTime, rangeGenerator, opens = 'right'){
    const ranges = rangeGenerator();
    $(domId).daterangepicker({
      startDate: initialStart,
      endDate: initialEnd,
      maxDate: moment(),
      dateLimit: {
        days: 60
      },
      opens,
      showDropdowns: true,
      showWeekNumbers: true,
      timePicker: showTime,
      timePickerIncrement: 5,
      timePicker12Hour: true,
      ranges,
      buttonClasses: ['btn', 'btn-sm'],
      applyClass: 'btn-primary',
      cancelClass: 'btn-default'
    }, callbackFun);
  },

  destroyTreemapSection() {
    $(this.graphPlaceholderId).children().remove();
  },

  renderTreemapSection() {
    this.destroyTreemapSection();
    const compiledGraph = this.compiledGraph(this.dimensionTreeMapModel);
    $(this.graphPlaceholderId).html(compiledGraph);
    // DRAW THE AXIS
    // Create the SVG Viewport
    var height = $('#axis-placeholder').height();
    var width = $('#axis-placeholder').width();
    var lineGraph = d3.select("#axis-placeholder").append("svg:svg").attr("width", width).attr("height", height);
    var myLine = lineGraph.append("svg:line").attr("x1", 0).attr("y1", height - 1).attr("x2", width).attr("y2", height - 1).style("stroke", "rgb(6,120,155)");
    var startValue = 0;
    var endValue = this.dimensionTreeMapModel.currentTotal;
    var middleValue = this.dimensionTreeMapModel.currentTotal/2;
    var startLabel = lineGraph.append("svg:text").attr("x", 0).attr("y", height - 6).text(startValue).attr("font-family", "SourceSansPro").attr("font-size", "20px");
    var endLabel = lineGraph.append("svg:text").attr("x", width / 2).attr("y", height - 6).text(middleValue).attr("font-family", "SourceSansPro").attr("font-size", "20px").attr("text-anchor",
        "middle");
    var endLabel = lineGraph.append("svg:text").attr("x", width).attr("y", height - 6).text(endValue).attr("font-family", "SourceSansPro").attr("font-size", "20px").attr("text-anchor", "end");

    var margin = 0;

    var self = this;

    var getChangeFactor = function (dataRow) {
      var factor = dataRow.percentageChange;
      if (self.dimensionTreeMapModel.heatmapMode === 'contributionChange') {
        factor = dataRow.contributionChange;
      }
      if (self.dimensionTreeMapModel.heatmapMode === 'contributionToOverallChange') {
        factor = dataRow.contributionToOverallChange;
      }
      return factor;
    };

    var getBackgroundColor = function (factor) {
      var opacity = Math.abs(factor / 25);
      var inverseMetric = self.dimensionTreeMapModel.inverseMetric;
      if((factor > 0 && !inverseMetric) || (factor < 0 && inverseMetric)){
        return "rgba(0,0,234," + opacity + ")";
      } else{
        return "rgba(234,0,0,"  + opacity + ")" ;
      }
    };

    var getTextColor = function (factor) {
      var opacity = Math.abs(factor / 25);
      if(opacity < 0.5){
        return "#000000";
      } else{
        return "#ffffff" ;
      }
    };

    var dimensions = this.dimensionTreeMapModel.dimensions;
    for (var i = 0; i < dimensions.length; i++) {
      var data = this.dimensionTreeMapModel.treeMapData[i];
      var dimension = dimensions[i];
      var dimensionPlaceHolderId = '#' + dimension + '-heatmap-placeholder';
      var height = $(dimensionPlaceHolderId).height();
      var width = $(dimensionPlaceHolderId).width();
      var treeMap = d3.layout.treemap().size([ width, height ]).sort(function(a, b) {
        return a.value - b.value;
      });

      var div = d3.select(dimensionPlaceHolderId).attr("class", "heatmap")
          .append("svg:svg").attr("width", width).attr("height", height).append("svg:g").attr("transform", "translate(.5,.5)");

      var nodes = treeMap.nodes(data).filter(function(d) {
        return !d.children;
      });
      var cell = div.selectAll("g").data(nodes).enter().append("svg:g").attr("class", "cell").attr("transform", function(d) {
        return "translate(" + d.x + "," + d.y + ")";
      }).on("click", function(d) {
        // return zoom(node == d.parent ? root : d.parent);
      }).on("mousemove", function(d) {
        const tooltipWidth = 200;
        const xPosition = d3.event.pageX - (tooltipWidth + 20);
        const yPosition = d3.event.pageY + 5;
        d3.select("#tooltip")
          .style("left", xPosition + "px")
          .style("top", yPosition + "px");
        d3.select("#tooltip #heading")
          .text(d.t);
        d3.select("#tooltip #percentageChange")
          .text(`${d.percentageChange}%`);
        d3.select("#tooltip #currentValue")
          .text(d.value);
        d3.select("#tooltip #contributionChange")
          .text(`${d.contributionChange}%`);
        d3.select("#tooltip #currentContribution")
          .text(d.currentContribution);
        d3.select("#tooltip #baselineContribution")
          .text(d.baselineContribution);
        d3.select("#tooltip #baselineValue")
          .text(d.baselineValue);
        d3.select("#tooltip").classed("hidden", false);
      }).on("mouseout", function() {
        d3.select("#tooltip").classed("hidden", true);
      });

      cell.append("svg:rect").attr("width", function(d) {
        return d.dx - 1
      }).attr("height", function(d) {
        return d.dy - 1
      }).style("fill", function(d) {
        var factor = getChangeFactor(d);
        return getBackgroundColor(factor);
      });

      cell.append("svg:text").attr("x", function(d) {
        return d.dx / 2;
      }).attr("y", function(d) {
        return d.dy / 2;
      }).attr("dy", ".35em").attr("text-anchor", "middle").text(function(d) {
        // TODO : add a condition here based on that show percentage change or contribution
        var factor = getChangeFactor(d);
        var text = d.t + '(' + factor + ')';

        //each character takes up 7 pixels on an average
        var estimatedTextLength = text.length * 7;
        if(estimatedTextLength > d.dx) {
          return text.substring(0, d.dx/7) + "..";
        } else {
          return text;
        }
      }).style("opacity", function(d) {
        d.w = this.getComputedTextLength();
        //uncomment this code to detect the average number of pixels per character. Currently its 6 but we use 7 to estimate the text length.
//        if(d.dx < d.w) {
//          text = d.t + '(' + d.percentageChange + ')'  ;
//          //console.log("text-length:"+ d.w + " cell-width:"+ d.dx + " text:" + text + " length:" + text.length + " pixels-per-letter:" + (d.w/text.length))
//        }
        return d.dx > d.w ? 1 : 0;
      }).style("fill", function(d){
        var factor = getChangeFactor(d);
        return getTextColor(factor);
      });

    }
    // anchor page to dimension tree map if exists
    $("#dimension-tree-map-placeholder").get(0).scrollIntoView()
  },

  calculateBaselineDate(dateType, offset) {
    const baseDate = this.dimensionTreeMapModel[dateType] || moment();
    return baseDate.clone().subtract(offset, 'days');
  },

  setupListenersForMode : function () {
    $("#percent_change a").click(() => {
      this.dimensionTreeMapModel.heatmapMode = "percentChange";
      this.renderTreemapSection();
    });
    $("#change_in_contribution").click(() => {
      this.dimensionTreeMapModel.heatmapMode = "contributionChange";
      this.renderTreemapSection();
    });
    $("#contribution_to_overall_change").click(() => {
      this.dimensionTreeMapModel.heatmapMode = "contributionToOverallChange";
      this.renderTreemapSection();
    });
  },
}


