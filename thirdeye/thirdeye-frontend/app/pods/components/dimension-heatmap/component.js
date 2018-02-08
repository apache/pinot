import Ember from 'ember';
import d3 from 'd3';

export default Ember.Component.extend({
  heatMapData: {},
  dimensions: Ember.computed.alias('heatMapData.dimensions'),
  metricName: Ember.computed.alias('heatMapData.metrics.firstObject'),
  inverseMetric:Ember.computed.alias('heatMapData.inverseMetric'),
  classNames: ['dimension-heatmap'],
  heatmapMode: 'Change in Contribution',

  // Copy pasted code from all Thirdeye UI
  treeMapData: Ember.computed(
    'dimensions',
    'dimensions.@each',
    'heatMapData',
    'metricName',
    function() {
      const dimensions = this.get('dimensions');
      const heatMapData = this.get('heatMapData');
      const metricName= this.get('metricName');
      const treeMapData = [];
      for (var i in dimensions) {
        var dimension = dimensions[i];
        var dataKey = `${metricName}.${dimension}`;
        var row = {"t": "0", "children": []};
        if (heatMapData.data && heatMapData.data[dataKey]) {
          const {
            dimensionValue: dimensionValueIndex,
            percentageChange: percentageChangeIndex,
            currentValue: currentValueIndex,
            baselineValue: baselineValueIndex,
            baselineContribution: baselineContributionIndex,
            contributionToOverallChange: contributionToOverallChangeIndex,
            currentContribution: currentContributionIndex,
            contributionDifference: contributionChangeIndex
          } = heatMapData.data[dataKey].schema.columnsToIndexMapping;

          for (var j in heatMapData.data[dataKey].responseData) {
            var record = heatMapData.data[dataKey].responseData[j];
            var item = {
              t: record[dimensionValueIndex],
              value: record[currentValueIndex],
              baselineValue: record[baselineValueIndex],
              currentContribution: record[currentContributionIndex],
              baselineContribution: record[baselineContributionIndex],
              contributionChange: record[contributionChangeIndex],
              percentageChange: record[percentageChangeIndex],
              contributionToOverallChange: record[contributionToOverallChangeIndex]
            };
            if (item.t) {
              row.children.push(item);
            }
          }
        }
        treeMapData.push(row);
      }
      return treeMapData;
    }
  ),

  didUpdateAttrs() {
    this._super(...arguments);

    d3.select('.dimension-heatmap').selectAll('svg').remove();
  },

  willRender() {
    this._super(...arguments);

    d3.select('.dimension-heatmap').selectAll('svg').remove();
  },

  didRender() {
    this._super(...arguments);

    // Copy pasted code from all Thirdeye UI
    const heatmapMode = this.get('heatmapMode');
    const inverseMetric = this.get('inverseMetric');

    var getChangeFactor = function (dataRow) {
      var factor = dataRow.percentageChange;
      if (heatmapMode === 'Change in Contribution') {
        factor = dataRow.contributionChange;
      }
      if (heatmapMode === 'Contribution to Overall Change') {
        factor = dataRow.contributionToOverallChange;
      }
      return factor;
    };

    var getBackgroundColor = function (factor) {
      var opacity = Math.abs(factor / 25);
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

    const dimensions = this.get('dimensions');
    const treeMapData = this.get('treeMapData');

    if (!dimensions) { return; }

    for (var i = 0; i < dimensions.length; i++) {
      var data = treeMapData[i];
      var dimension = dimensions[i];
      var dimensionPlaceHolderId = '#' + dimension + '-heatmap-placeholder';
      var height = Ember.$(dimensionPlaceHolderId).height();
      var width = Ember.$(dimensionPlaceHolderId).width();
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
      // }).on("click", function(d) {
        // return zoom(node == d.parent ? root : d.parent);
      }).on("mousemove", function(d) {

        if (!d.percentageChange) { return; }
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
        return Math.max(d.dx - 1, 0);
      }).attr("height", function(d) {
        return Math.max(d.dy - 1, 0);
      }).style("fill", function(d) {
        var factor = getChangeFactor(d);
        return getBackgroundColor(factor);
      });

      cell.append('svg:text').attr("x", function(d) {
        return d.dx / 2;
      }).attr("y", function(d) {
        return d.dy / 2;
      }).attr("dy", ".35em").attr("text-anchor", "middle").text(function(d) {
        var factor = getChangeFactor(d);
        var text = d.t + '(' + factor + ')';

        //each character takes up 7 pixels on an average
        var estimatedTextLength = text.length * 7;
        if(estimatedTextLength > d.dx) {
          return text.substring(0, d.dx/7) + "..";
        } else {
          return text;
        }
      }).style('opacity', function(d) {
        d.w = this.getComputedTextLength();
        return d.dx > d.w ? 1 : 0;
      }).style('fill', function(d){
        var factor = getChangeFactor(d);
        return getTextColor(factor);
      });
    }
  }
});
