function d3Line() {
  var margin = {top: 0, right: 0, bottom: 0, left: 0},
      width = 960,
      height = 500,
      dotRadius = function() { return 2.5 },
      color = d3.scale.category10().range(),
      id = Math.floor(Math.random() * 10000), //Create semi-unique ID incase user doesn't select one
      x = d3.scale.linear(),
      y = d3.scale.linear(),
      dispatch = d3.dispatch("pointMouseover", "pointMouseout"),
      x0, y0;


  function chart(selection) {
    selection.each(function(data) {
      var seriesData = data.map(function(d) { return d.data });

      x0 = x0 || x;
      y0 = y0 || y;

      //TODO: reconsider points {x: #, y: #} instead of [x,y]
      //TODO: data accessors so above won't really matter, but need to decide for internal use

      //add series data to each point for future ease of use
      data = data.map(function(series, i) {
        series.data = series.data.map(function(point) {
          point.series = i;
          return point;
        });
        return series;
      });


      x   .domain(d3.extent(d3.merge(seriesData), function(d) { return d[0] } ))
          .range([0, width - margin.left - margin.right]);

      y   .domain(d3.extent(d3.merge(seriesData), function(d) { return d[1] } ))
          .range([height - margin.top - margin.bottom, 0]);


      var vertices = d3.merge(data.map(function(line, lineIndex) {
            return line.data.map(function(point, pointIndex) {
              var pointKey = line.label + '-' + point[0];
              return [x(point[0]), y(point[1]), lineIndex, pointIndex]; //adding series index to point because data is being flattened
            })
          })
      );


      var wrap = d3.select(this).selectAll('g.d3line').data([data]);
      var gEnter = wrap.enter().append('g').attr('class', 'd3line').append('g');

      gEnter.append('g').attr('class', 'lines');
      gEnter.append('g').attr('class', 'point-clips');
      gEnter.append('g').attr('class', 'point-paths');


      var g = wrap.select('g')
          .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');



      var voronoiClip =  gEnter.append('g').attr('class', 'voronoi-clip')
        .append('clipPath')
          .attr('id', 'voronoi-clip-path-' + id) //this id should probably be set on update, unless ID is always set before render
        .append('rect');
      wrap.select('.voronoi-clip rect')
          .attr('x', -10)
          .attr('y', -10)
          .attr('width', width - margin.left - margin.right + 20)
          .attr('height', height - margin.top - margin.bottom + 20);
      wrap.select('.point-paths')
          .attr('clip-path', 'url(#voronoi-clip-path-' + id + ')');


      //var pointClips = wrap.select('.point-clips').selectAll('clipPath') // **BROWSER BUG** can't reselect camel cased elements
      var pointClips = wrap.select('.point-clips').selectAll('.clip-path')
          .data(vertices);
      pointClips.enter().append('clipPath').attr('class', 'clip-path')
        .append('circle')
          .attr('r', 25);
      pointClips.exit().remove();
      pointClips
          .attr('id', function(d, i) { return 'clip-' + id + '-' + d[2] + '-' + d[3] })
          .attr('transform', function(d) { return 'translate(' + d[0] + ',' + d[1] + ')' });


      var voronoi = d3.geom.voronoi(vertices).map(function(d,i) {
          return { 'data': d, 'series': vertices[i][2], 'point': vertices[i][3] }
      });

      //TODO: Add very small amount of noise to prevent duplicates
      var pointPaths = wrap.select('.point-paths').selectAll('path')
          .data(voronoi);
      pointPaths.enter().append('path')
          .attr('class', function(d,i) { return 'path-' + i; });
      pointPaths.exit().remove();
      pointPaths
          .attr('clip-path', function(d) { return 'url(#clip-' + id + '-' + d.series + '-' + d.point + ')'; })
          .attr('d', function(d) { return 'M' + d.data.join(',') + 'Z'; })
          .on('mouseover', function(d) {
            dispatch.pointMouseover({
              point: data[d.series].data[d.point],
              series: data[d.series],
              pos: [x(data[d.series].data[d.point][0]) + margin.left, y(data[d.series].data[d.point][1]) + margin.top],
              pointIndex: d.point,
              seriesIndex: d.series
            });
          })
          .on('mouseout', function(d) {
            dispatch.pointMouseout({
              point: d,
              series: data[d.series],
              pointIndex: d.point,
              seriesIndex: d.series
            });
          });


      dispatch.on('pointMouseover.point', function(d) { 
        wrap.select('.line-' + d.seriesIndex + ' .point-' + d.pointIndex)
          .classed('hover', true);
      });

      dispatch.on('pointMouseout.point', function(d) { 
        wrap.select('.line-' + d.seriesIndex + ' .point-' + d.pointIndex)
          .classed('hover', false);
      });



      var lines = wrap.select('.lines').selectAll('.line')
          .data(function(d) { return d }, function(d) { return d.label });
      lines.enter().append('g')
          .style('stroke-opacity', 1e-6)
          .style('fill-opacity', 1e-6);
      d3.transition(lines.exit())
          .style('stroke-opacity', 1e-6)
          .style('fill-opacity', 1e-6)
          .remove();
      lines.attr('class', function(d,i) { return 'line line-' + i })
          .classed('hover', function(d) { return d.hover })
          .style('fill', function(d,i) { return color[i % 10] })
          .style('stroke', function(d,i) { return color[i % 10] })
      d3.transition(lines)
          .style('stroke-opacity', 1)
          .style('fill-opacity', .5);


      var paths = lines.selectAll('path')
          .data(function(d, i) { return [d.data] });
      paths.enter().append('path')
          .attr('d', d3.svg.line()
            .x(function(d) { return x0(d[0]) })
            .y(function(d) { return y0(d[1]) })
          );
      paths.exit().remove();
      d3.transition(paths)
          .attr('d', d3.svg.line()
            .x(function(d) { return x(d[0]) })
            .y(function(d) { return y(d[1]) })
          );


      var points = lines.selectAll('circle.point')
          .data(function(d) { return d.data });
      points.enter().append('circle')
          .attr('cx', function(d) { return x0(d[0]) })
          .attr('cy', function(d) { return y0(d[1]) });
      points.exit().remove();
      points.attr('class', function(d,i) { return 'point point-' + i });
      d3.transition(points)
          .attr('cx', function(d) { return x(d[0]) })
          .attr('cy', function(d) { return y(d[1]) })
          .attr('r', dotRadius());

    });

    x0 = x;
    y0 = y;

    return chart;
  }



  chart.dispatch = dispatch;

  chart.margin = function(_) {
    if (!arguments.length) return margin;
    margin = _;
    return chart;
  };

  chart.width = function(_) {
    if (!arguments.length) return width;
    width = _;
    return chart;
  };

  chart.height = function(_) {
    if (!arguments.length) return height;
    height = _;
    return chart;
  };

  chart.dotRadius = function(_) {
    if (!arguments.length) return dotRadius;
    dotRadius = d3.functor(_);
    return chart;
  };

  chart.color = function(_) {
    if (!arguments.length) return color;
    color = _;
    return chart;
  };

  chart.id = function(_) {
    if (!arguments.length) return id;
    id = _;
    return chart;
  };


  return chart;
}