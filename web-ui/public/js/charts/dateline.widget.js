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

  $.widget( "inviso.dateline", {
    options: {
      margin: { top: 0, right: 0, bottom: 30, left: 115},
      extent: [moment().subtract(3,'days').toDate(), moment().toDate()],
      start: moment().subtract(30, 'days').toDate(),
      end: moment().toDate()
    },

    _create: function() {
      var self = this;
      var $e = $(this.element);
      var o = this.options;
      var m = o.margin;

      this.width = $e.width() - m.left - m.right;
      this.height = $e.height() - m.top - m.bottom;

      this.x = d3.time.scale()
        .domain([o.start, o.end])
        .range([0, this.width]);

      this.brush = d3.svg.brush()
        .x(this.x)
        .extent(o.extent)
        .on("brushend", function() {
          self._brushend();
        });

      this.svg = d3.select(this.element[0]).append("svg")
        .attr("width", this.width + m.left + m.right)
        .attr("height", this.height + m.top + m.bottom);

      this.svgg= this.svg.append("g")
          .attr("transform", "translate(" + m.left + "," + m.top + ")");

      this.background = this.svgg.append("rect")
        .attr("class", "grid-background")
        .attr("width", this.width)
        .attr("height", this.height);

      this.grid = this.svgg.append("g")
        .attr("class", "x grid")
        .attr("transform", "translate(0," + this.height + ")");

      this.axis = this.svgg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + this.height + ")");

      this.gBrush = this.svgg.append("g")
        .attr("class", "brush")
        .call(this.brush)
        .call(this.brush.event);

      this.gBrush.selectAll("rect")
        .attr("height", this.height);


      this._updateLayout();

      //Resize operation
      var resize = _.debounce(function(){
        self._updateLayout();
      }, 500);
      $(window).on('resize', resize);
    },

    _updateLayout: function() {
      var self = this;
      var $e = $(this.element);
      var m = this.options.margin;

      this.width = $e.width() - m.left - m.right;
      this.height = $e.height() - m.top - m.bottom;

      this.svg
        .attr("width", this.width + m.left + m.right)
        .attr("height", this.height + m.top + m.bottom);

      this.x.range([0, this.width]);

      this.background
        .attr("width", this.width)
        .attr("height", this.height);

      this.grid.call(d3.svg.axis()
          .scale(this.x)
          .orient("bottom")
          .ticks(d3.time.hours, 24)
          .tickSize(-this.height)
          .tickFormat(""))
        .selectAll(".tick")
          .classed("minor", function(d) { return d.getHours(); });

      this.axis.call(d3.svg.axis()
          .scale(this.x)
          .orient("bottom")
          .ticks(d3.time.days)
          .tickFormat(d3.time.format("%m/%-e"))
          .tickPadding(0))
        .selectAll("text")
          .attr("x", 2)
          .attr("transform", 'rotate(45)')
          .style("text-anchor", null);

    },

    _brushend: function(){
      this._trigger( "brush", null, { range: this.brush.extent() } );
    },

    extent: function(extent) {
      this.options.extent = extent;
      this.reset();
    },

    reset: function() {
      this.brush.extent(this.options.extent);
      this.gBrush.call(this.brush);
    },

  });
})( jQuery, window, document );
