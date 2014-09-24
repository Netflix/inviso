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

String.prototype.contains = function(it) {
  return this.indexOf(it) != -1;
};

String.prototype.startsWith = function(it) {
  return this.indexOf(it) === 0;
};

Array.prototype.extend = function (other) {
    other.forEach(function(v) {this.push(v);}, this);
    return this;
};

Array.prototype.clone = function() {
  return this.slice(0);
};

Array.prototype.first = function() {
  return this[0];
};

Array.prototype.random = function() {
  return this[Math.floor(Math.random() * this.length)];
};

Array.prototype.transpose = function() {
  var _this = this;
  return this[0].map(function (_, c) { return _this.map(function (r) { return r[c]; }); });
};

Array.prototype.chain = function chain( delay ) {
  var tasks = this, pos = 0, delay = delay || 100;
  setTimeout( function() {
    tasks[pos++]();
    if (pos<tasks.length) setTimeout( arguments.callee, delay );
  }, delay );
  return this;
};

Array.prototype.chunk = function(chunkSize) {
  var R = [];
  for (var i=0; i<this.length; i+=chunkSize)
    R.push(this.slice(i,i+chunkSize));
  return R;
};
