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

_.templateSettings = {
  interpolate: /\{\{(.+?)\}\}/g
};

window.settings = {
  directHistoryPath: true,
  cluster: {
    max: 50,
    default: 'cluster_1',
    applicationLookback: 60*60*1000,
    capacityLookback: 3*24*60*60*1000
  }
};

function initElasticSearch(callback) {
  elasticSearchUrl = 'http://' + window.location.hostname + ':9200/inviso/config/';
  elasticSearchHosts = [
    {host: window.location.hostname, port: 9200}
  ];

  if(callback != null) {
    callback(elasticSearchHosts);
  }
}
