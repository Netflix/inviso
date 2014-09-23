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

function hhmmss(ms) {
  var seconds = parseInt(ms / 1000);
  var hh = Math.floor(seconds / 3600);
  var mm = Math.floor((seconds - (hh * 3600)) / 60);
  var ss = seconds - (hh * 3600) - (mm * 60);

  if (hh < 10) {hh = '0' + hh;}
  if (mm < 10) {mm = '0' + mm;}
  if (ss < 10) {ss = '0' + ss;}

  return hh + ':' + mm + ':' + ss;
}

function asArray(object) {
  if($.isArray(object)) {
    return object;
  } else {
    return [object];
  }
}

function time_format(epoch) {
  return moment(epoch).format('YYYY-MM-DD HH:mm:ss (UTC ZZ)');
}

function createSpinner($div) {
  $div.append('<div class="spinner"><img src="img/spinner.gif"/></dib>');
}

function removeSpinner($div) {
  $div.find('.spinner').remove();
}
