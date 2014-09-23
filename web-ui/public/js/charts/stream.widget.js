/*
 *
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

;(function ( $, window, document, undefined ) {

  $.widget( "inviso.stream", {
    options: {
      margin: { top: 20, right: 0, bottom: 20, left: 100 },
      title: null,
      series: {},
      offset: "wiggle",
      interpolation: "basis",
      scale: { x:d3.scale.linear, y:d3.scale.linear },
      filter: _.identity,
      brush: false,
      color: function(d) {
        if(this.c === undefined) {
          this.c = d3.scale.linear().range(["#90cde7", "#2d4fca"]);
        }
        return this.c(Math.random());
      },
      tooltip: function(d) {return d.id;},
      highlightColors: [
        d3.scale.linear().range(["#fcd958", "#a79325"]),
        d3.scale.linear().range(["#9e59fe", "#512f86"]),
        d3.scale.linear().range(["#78f44b", "#3f7b27"]),
        d3.scale.linear().range(["#fe59c5", "#76295f"]),
        d3.scale.linear().range(["#f76874", "#6d2f35"]),
        d3.scale.linear().range(["#feae59", "#966031"]),
        d3.scale.linear().range(["#59feb7", "#329366"])
        ]
    },

    _create: function() {
      var self = this;
      var $e = $(this.element);

      var m = this.options.margin;

      this.width = $e.width();
      this.height = $e.height();

      this.svg = d3.select(this.element[0]).append("svg")
        .attr("width", this.width)
        .attr("height", this.height);

      this.chart = this.svg.append("g")
        .attr("class", "stream-chart")
        .attr("transform", "translate("+m.left+","+m.top+")");

      this.title = this.svg.append('g')
        .attr("class", "chart-title")
        .attr("transform", "translate("+(m.left+10)+","+m.top+")");

      this.x = this.options.scale.x()
        .domain([Date.now() - 2*60*60*1000, Date.now()])
        .range([0,this.width - m.left - m.right ]);

      this.xAxis = d3.svg.axis()
        .scale(this.x)
        .ticks(this.width/300)
        .tickFormat(function(d){ return moment(d).format('YYYY-MM-DD HH:mm:ss (UTC ZZ)'); })
        .orient("bottom");

      this.svg.append('g')
        .attr("class","x axis")
        .attr("transform", "translate("+m.left+"," + (this.height - m.bottom) + ")")
        .call(this.xAxis);

      this.y = this.options.scale.y()
        .domain([0, 100])
        .range([this.height - m.top - m.bottom, 0]);

      this.yAxis = d3.svg.axis()
        .scale(this.y)
        .orient("left");

      this.svg.append('g')
        .attr("class","y axis")
        .attr("transform", "translate("+m.left+","+m.top+")")
        .call(this.yAxis);

      this.legend = this.svg.append('g')
        .attr('class', 'stream-legend');

      if(this.options.title != null) {
        this.title.append('text')
        .attr('y', 20)
        .text(this.options.title);
      }

      var resize = _.debounce(function(){
        self.width = $e.width();
        self.height = $e.height();

        self.svg
          .attr("width", self.width)
          .attr("height", self.height);
        $e.stream('update');
      }, 500);
      $(window).on('resize', resize);
    },

    update: function(series) {
      if(series == null) {
        series = this.series;
      }

      if(_.isEmpty(series.data)) {
        this.chart.selectAll("path").remove();
        return;
      }

      this._verify(series.data);

      var self = this;
      this.series = series;
      this.stack = d3.layout.stack().offset(this.options.offset);

      var layers = this.layers = this._stack_bottom(this.stack(this.options.filter(series.data)));

      this._updateAxes();

      this._updateBrush();

      this.area = d3.svg.area()
        .interpolate(self.options.interpolation)
        .x(function(d) { return self.x(d.x); })
        .y0(function(d) { return self.y(d.y0); })
        .y1(function(d) { return self.y(d.y0 + d.y); });

      var selection = this.chart.selectAll("path").data(layers, function(d) {
        return d[0].id;
        });

      selection.enter().append("path")
          .attr("d", this.area)
          .style("fill", function(d) {
            d.color = self.options.color(d);
            return d.color;
          })
          .on("click", function(d, i) {
            if(self.selected == d) {
              self.selected = null;
            } else {
              self.selected = d;
            }

            self.chart.selectAll("path").style("fill", function(d2) {
              d3.event.stopPropagation();

              if(d != d2 && self.selected != null) {
                return "#cdd1d6";
              } else {
                return d2.color;
              }
            });
          })
          .on("mouseover", function(d){
            var data = _.last(_.filter(_.flatten(d), function(v) { return v.id != null; }));

            if(!d.tooltip) {
              d.tooltip = true;
              $(this).tipsy({trigger: 'manual', gravity: 'n', track: true, offset: 15, html:true, title: function(){ return self.options.tooltip(data); } });
            }

            $(this).tipsy('show');
          })
          .on("mousemove", function(d){
            var data = _.last(_.filter(_.flatten(d), function(v) { return v.id != null; }));

            $(this).tipsy('setTitle', self.options.tooltip(data));
          })
          .on("mouseout", function(d){
            $(this).tipsy('hide');
          });

      selection.attr("d", this.area)
      .style("fill", function(d) {
        d.color = self.options.color(d);
        return d.color;
      });

      selection.exit().remove();
    },

    _updateAxes: function() {
      var layers = this.layers;
      var m = this.options.margin;

      var start = d3.min(layers, function(layer) { return d3.min(layer, function(d) { return d.x; }); });
      var stop  = d3.max(layers, function(layer) { return d3.max(layer, function(d) { return d.x; }); });
      this.x
        .domain([start, stop])
        .range([0, this.width - m.left - m.right]);

      this.xAxis.ticks(this.width/300);

      this.svg.selectAll('.x.axis')
        .attr("transform", "translate("+m.left+"," + (this.height - m.bottom) + ")")
        .call(this.xAxis);

      var max = d3.max(layers, function(layer) { return d3.max(layer, function(d) { return d.y0 + d.y; }); });

      this.y
        .domain([0.1, max])
        .range([this.height - m.top - m.bottom, 0]);

      if(this.options.scale.y == d3.scale.log) {
        this.y.base(2);
        this.y.clamp(true);
      }

      this.svg.selectAll('.y.axis').call(this.yAxis);
    },

    _updateBrush: function() {
      if(!this.options.brush) {
        return;
      }

      var self = this;
      var m = this.options.margin;

      if(this.brush == null) {
        this.brush = d3.svg.brush()
          .x(this.x)
          .extent([Date.now() - 1*60*60*1000, Date.now()])
          .on("brushend", function(){
            self._trigger( "brush", null, { range: self.brush.extent()} );
          });

        this.brushg = this.svg.append('g')
          .attr("class", "brush")
          .attr("transform", "translate("+m.left+","+m.top+")")
          .call(this.brush);
      }

      this.brushg.selectAll("rect")
        .attr("height", this.height - m.top - m.bottom);
    },

    _transition: function() {
      var self = this;

      this.chart.selectAll('path').data(function() {
        self.layers = self.stack(self.series.data);

        self._updateAxes();
        return self.layers;
      }).transition()
      .duration(1500)
      .attr("d", this.area);
    },

    highlight: function(term) {
      var self = this;
      var hl = this.options.highlightColors;
      var terms = _.filter(_.map(term.split(" "), function(v){return v.trim();}), function(v){return v!=="";});

      if(term == null || term.trim() === '') {
        self.options.color = self.color || self.options.color;
        self.color = null;
      } else {
        self.color = self.color || self.options.color;
        self.options.color = function(d) {
          var values = _.values(_.reduce(_.flatten(d), function(m, p) { return _.extend(m, p); }, {}));

          var match = -1;
          for(var i=0; i<values.length; i++) {
            for(var j=0; j<terms.length; j++) {
              if(_.isString(values[i]) && values[i].contains(terms[j])) {
                match = j;
                break;
              }

              if(match >= 0) {
                break;
              }
            }
          }

          if(match >= 0) {
            return hl[match%hl.length](Math.random());
          } else {
            return d.color || self.color(d);
          }
        };
      }

      this.chart.selectAll('path').style('fill', this.options.color);

      var selection = this.legend.selectAll('g.stream-legend-item').data(terms, function(d){return d;});

      var groups = selection.enter()
        .append('g')
        .attr('class', 'stream-legend-item')
        .attr('transform', function(d, i){return 'translate('+(self.width)+',10)';});

      groups.append('circle')
        .attr({
          cx: 0,
          cy: 0,
          r: 7,
          fill: function(d,i) { return hl[i%hl.length](0.5);}
        });

      groups.append('text')
        .attr({
          x: 15,
          y: 3
        })
        .text(function(d){return d;});

      var totalLegendWidth = 0;

      selection
        .transition().duration(500)
        .attr('transform', function(d, i){
          totalLegendWidth += this.getBBox().width + 15;
          return 'translate('+(self.width - 100 - totalLegendWidth)+',10)';}
        );

      selection.select('circle')
        .attr('fill', function(d,i) { return hl[i%hl.length](0.5); });

      selection.exit().transition().duration(500)
      .attr('transform', function(d, i){
        return 'translate('+self.width+',10)';}
      )
      .remove();
    },

    filter: function(term) {
      var self = this;

      if(term == null || term.trim() === '') {
        this.options.filter = _.identity;
        this.update();
        return;
      }

      var terms = _.filter(_.map(term.split(" "), function(v){return v.trim();}), function(v){return v!=="";});

      this.options.filter = function(layers) {
        return _.filter(layers, function(d) {
          var pred = function(v){ return _.isString(v) && _.reduce(terms, function(m, t) { return v.contains(t) || m; }, false); };

          for(var i=0; i<d.length; i++) {
            if(_.find(_.values(d[i]), pred)) {
              return true;
            }
          }

          return false;
        });
      };

      this.update();
    },

    _verify: function(layers) {
      _.each(layers, function(layer) {
        var last = -Infinity;
        _.each(layer, function(period){
          if(period.x <= last)
            throw "Period error: " + period.id +" "+period.x+" <= "+last;
          else
            last = period.x;
        });
      });
    },

    _stack_bottom: function(layers) {


      return layers;
    },

    _setOption: function( key, value ) {
      if( key === "offset" ) {
        this.stack.offset(value);

        this._transition();
      }
      this._super( key, value );
    },

    _setOptions: function( options ) {
      this._super( options );
      this.refresh();
    },

    refresh: function() {
    },

  });

})( jQuery, window, document );
