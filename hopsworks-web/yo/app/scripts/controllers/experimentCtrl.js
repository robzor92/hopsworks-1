/*
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
'use strict';
/*
 * Controller for the Experiments service
 */
angular.module('hopsWorksApp')
    .controller('ExperimentCtrl', ['$scope', '$timeout', 'growl', '$window', 'MembersService', 'UserService', 'ModalService', 'ProjectService', 'ExperimentService', 'TensorBoardService', '$interval',
        '$routeParams', '$route', '$sce', '$window',
        function($scope, $timeout, growl, $window, MembersService, UserService, ModalService, ProjectService, ExperimentService, TensorBoardService, $interval,
            $routeParams, $route, $sce, $window) {

            var self = this;

            self.deleted = {}

            self.pageSize = 12;
            self.currentPage = 1;
            self.totalItems = 0;

            self.currentResultPage = {};

            self.sortType = 'start';
            self.orderBy = 'desc';
            self.reverse = true;

            self.resultSortType = {};
            self.resultTotalItems = {};
            self.resultOrderBy = {};
            self.resultsReverse = {};

            self.inModalView = false;

            self.projectId = $routeParams.projectID;

            self.memberSelected = {};

            self.experiments = [];

            self.loading = false;
            self.loadingText = "";

            self.loaded = false;

            self.experimentsFilter = "";

            self.query = "";

            self.membersList = [];
            self.members = [];
            self.userEmail = "";

            self.updating = false;
            self.expandExperiment = {};

            self.experimentsToDate = new Date();
            self.experimentsToDate.setMinutes(self.experimentsToDate.getMinutes() + 60*24);
            self.experimentsFromDate = new Date();
            self.experimentsFromDate.setMinutes(self.experimentsToDate.getMinutes() - 60*24*30);

            var startLoading = function(label) {
                self.loading = true;
                self.loadingText = label;
            };
            var stopLoading = function() {
                self.loading = false;
                self.loadingText = "";
            };

            self.experimentsOrder = function () {
                if (self.reverse) {
                    self.orderBy = "desc";
                } else {
                    self.orderBy = "asc";
                }
            };

            self.sortByExperiments = function(type) {
                if(self.sortType !== type) {
                    self.reverse = true;
                } else {
                    self.reverse = !self.reverse; //if true make it false and vice versa
                }
                self.sortType = type;
                self.experimentsOrder();
                self.getExperiments();
            };

            self.sortByResults = function(type, experiment) {
                if(self.resultSortType[experiment.id] !== type) {
                    self.resultsReverse[experiment.id] = true;
                } else {
                    self.resultsReverse[experiment.id] = !self.resultsReverse[experiment.id]; //if true make it false and vice versa
                }
                self.resultSortType[experiment.id] = type;
                self.getResults(experiment, true);
            };

            self.deleteExperiment = function (id) {
                ModalService.confirm('sm', 'Delete Experiment?',
                    'WARNING: This will remove the directory in your Experiments dataset containing your Experiment output, it will also be removed from this view. This action can not be undone.')
                    .then(function (success) {
                        startLoading("Deleting Experiment...");
                        ExperimentService.deleteExperiment(self.projectId, id).then(
                            function(success) {
                                  stopLoading();
                                  self.deleted[id] = true;
                                  self.getExperiments();
                            },
                            function(error) {
                                stopLoading();
                                if (typeof error.data.usrMsg !== 'undefined') {
                                    growl.error(error.data.usrMsg, {title: error.data.errorMsg, ttl: 8000});
                                } else {
                                    growl.error("", {title: error.data.errorMsg, ttl: 8000});
                                }
                            });
                    }, function (error) {
                    });
            };

            self.viewExperiment = function (experiment) {
                self.inModalView = true;
                ModalService.viewExperimentInfo('lg', self.projectId, experiment).then(
                function (success) {
                    self.inModalView = false;
                    self.getExperiments();
                }, function (error) {
                    self.inModalView = false;
                    self.getExperiments();
                });
            };

            self.viewMonitor = function (experiment) {
              if(experiment.jobName) {
                $window.open('project/' + self.projectId + '/jobMonitor-job/' + experiment.jobName, '_blank');
              } else {
                $window.open('project/' + self.projectId + '/jobMonitor-app/' + experiment.appId + '/true/jupyter', '_blank');
              }
            };

            self.buildQuery = function() {
                var offset = self.pageSize * (self.currentPage - 1);
                self.query = "";
                if(self.experimentsFilter !== "") {
                    self.query = '?filter_by=name:' + self.experimentsFilter + "&filter_by=date_start_lt:" + self.experimentsToDate.toISOString().replace('Z','')
                        + "&filter_by=date_start_gt:" + self.experimentsFromDate.toISOString().replace('Z','');
                } else {
                    self.query = '?filter_by=date_start_lt:' + self.experimentsToDate.toISOString().replace('Z','')
                        + "&filter_by=date_start_gt:" + self.experimentsFromDate.toISOString().replace('Z','');
                }
                if(self.memberSelected.name !== 'All Members') {
                    self.query = self.query + '&filter_by=user:' + self.memberSelected.uid;
                }
                self.query = self.query + '&sort_by=' + self.sortType + ':' + self.orderBy + '&offset=' + offset + '&limit=' + self.pageSize;
            };

            self.getExperiments = function(loadingText) {

                if(loadingText) {
                    startLoading(loadingText);
                }
                self.buildQuery();
                self.updating = true;
                ExperimentService.getAll(self.projectId, self.query).then(
                    function(success) {
                        if(loadingText) {
                            stopLoading();
                        }
                        if(success.data.items) {
                          for(var i = 0; success.data.items.length > i; i++) {
                            if(success.data.items[i].id in self.deleted) {
                               console.log('removing element currently being deleted')
                               success.data.items.splice(i, 1);
                            }
                          }
                        }
                        self.updating = false;
                        if(success.data.count !== self.totalItems) {
                            console.log('overwrite')
                            self.experiments = success.data.items;
                        } else {
                        var i=0;
                            //Construct an array of jobs and their latest execution info
                            angular.forEach(success.data.items, function (experiment, key) {
                                if(typeof self.experiments[i] === 'undefined'){
                                    self.experiments[i] = {};
                                }
                                self.experiments[i].name = experiment.name;
                                self.experiments[i].metric = experiment.metric;
                                self.experiments[i].user = experiment.user;
                                self.experiments[i].start = experiment.start;
                                self.experiments[i].end = experiment.end;
                                self.experiments[i].state = experiment.state;
                                i++;
                            });
                        }
                        self.totalItems = success.data.count;
                        self.loaded = true;
                    },
                    function(error) {
                        if(loadingText) {
                            stopLoading();
                        }
                        self.loaded = true;
                        self.updating = false;
                        if (typeof error.data.usrMsg !== 'undefined') {
                            growl.error(error.data.usrMsg, {title: error.data.errorMsg, ttl: 8000});
                        } else {
                            growl.error("", {title: error.data.errorMsg, ttl: 8000});
                        }
                    });
            };

            /**
             * Helper function for redirecting to another project page
             *
             * @param serviceName project page
             */
            self.goToExperiment = function (experiment_id) {
                $window.open('project/' + self.projectId + '/datasets/Experiments/' + experiment_id, '_blank');
            };

            self.goToModel = function (model) {
                var modelSplit = model.split('_')
                $window.open('project/' + self.projectId + '/datasets/Models/' + modelSplit[0] + '/' + modelSplit[1], '_blank');
            };

            self.init = function () {
              UserService.profile().then(
                function (success) {
                  self.userEmail = success.data.email;
                  self.getMembers();
                },
                function (error) {
                    if (typeof error.data.usrMsg !== 'undefined') {
                        growl.error(error.data.usrMsg, {title: error.data.errorMsg, ttl: 8000});
                    } else {
                        growl.error("", {title: error.data.errorMsg, ttl: 8000});
                    }
                });
            };

            self.getMembers = function () {
              MembersService.query({id: self.projectId}).$promise.then(
                function (success) {
                  self.members = success;
                  if(self.members.length > 0) {
                    //Get current user team role
                    self.members.forEach(function (member) {
                        if(member.user.email !== 'serving@hopsworks.se') {
                            self.membersList.push({'name': member.user.fname + ' ' + member.user.lname, 'uid': member.user.uid, 'email': member.user.email});
                        }
                    });


                    self.membersList.push({'name': 'All Members'})

                    for(var i = 0; i < self.membersList.length; i++) {
                        if(self.membersList[i].email === self.userEmail) {
                            self.memberSelected = self.membersList[i];
                            break;
                        }
                    }
                  }
                  self.getExperiments('Loading Experiments...');
                },
                function (error) {
                    if (typeof error.data.usrMsg !== 'undefined') {
                        growl.error(error.data.usrMsg, {title: error.data.errorMsg, ttl: 8000});
                    } else {
                        growl.error("", {title: error.data.errorMsg, ttl: 8000});
                    }
                });
            };

            self.init();

            self.getNewExperimentPage = function() {
                self.getExperiments();
            };


            $scope.$on('$destroy', function () {
              $interval.cancel(self.poller);
            });

            var startPolling = function () {
              self.poller = $interval(function () {
                if(!self.updating && !self.inModalView) {
                  self.getExperiments();
                }
              }, 15000);
            };
            startPolling();

            self.hp_headers = {};
            self.metric_headers = {};
            self.all_headers = {};
            self.experimentResults = {};

            self.initExpansionTable = function(experiment) {
                var experimentId = experiment.id;
                self.experimentResults[experimentId] = [];

                if (experiment['results'] && experiment['results']['count'] > 0) {
                    var results = experiment['results']['combinations'];
                    if (results) {
                        if (results[0]['parameters']) {
                            self.hp_headers[experimentId] = [];
                            for(var key in results[0]['parameters']) {
                                self.hp_headers[experimentId].push(key)
                            }
                        }
                        if(results[0]['metrics']) {
                            self.metric_headers[experimentId] = [];
                            for(var key in results[0]['metrics']) {
                                self.metric_headers[experimentId].push(key);
                            }
                        }
                        self.all_headers[experimentId] = self.hp_headers[experimentId].concat(self.metric_headers[experimentId]);

                        for (var i = 0; i < results.length; i++) {
                            var tmp = []
                            if (results[i]['parameters']) {
                                for(var key in results[i]['parameters']) {
                                    tmp.push({
                                        'data': results[i]['parameters'][key],
                                        'file': false
                                    })
                                }
                            }
                            if (results[i]['metrics']) {
                                for(var key in results[i]['metrics']) {
                                    var is_file = String(results[i]['metrics'][key]).indexOf('/') > -1;
                                    tmp.push({
                                        'data': results[i]['metrics'][key],
                                        'file': is_file
                                    });
                                }
                            }
                            if(!self.experimentResults[experimentId]) {
                                self.experimentResults[experimentId] = [];
                            }
                            self.experimentResults[experimentId].push({
                                'row': tmp
                            });
                        }
                    }
                }
            };

            self.viewFile = function(filePath) {
                ModalService.filePreview('lgg', filePath.replace(/^.*[\\\/]/, ''), filePath, self.projectId, "head").then(
                    function(success) {},
                    function(error) {});
            };

            self.getResults = function(experiment, expand) {
                if(!self.expandExperiment[experiment.id] || expand) {
                    var query = self.buildResultsQuery(experiment);
                    ExperimentService.get(self.projectId, experiment.id, query).then(
                        function(success) {
                            self.initExpansionTable(success.data);
                            if(success.data.results.count) {
                              self.resultTotalItems[experiment.id] = success.data.results.count;
                            }
                            self.expandExperiment[experiment.id] = true;
                        },
                        function(error) {
                            if (typeof error.data.usrMsg !== 'undefined') {
                                growl.error(error.data.usrMsg, {
                                    title: error.data.errorMsg,
                                    ttl: 8000
                                });
                            } else {
                                growl.error("", {
                                    title: error.data.errorMsg,
                                    ttl: 8000
                                });
                            }
                        });
                } else if(!expand) {
                    delete self.expandExperiment[experiment.id];
                }
            };

            self.goToDirectory = function (path) {
                $window.open('project/' + self.projectId + '/datasets/' + path, '_blank');
            };

            self.buildModelLink = function(modelName) {
                if(modelName) {
                    var modelSplit = modelName.split('_');
                    var modelName = modelSplit[0];
                    var modelVersion = modelSplit[1];
                    return 'Models/' + modelName + '/' + modelVersion;
                }
            };

            self.buildResultsQuery = function(experiment) {

                if (self.resultsReverse[experiment.id]) {
                    self.resultOrderBy[experiment.id] = "desc";
                } else {
                    self.resultOrderBy[experiment.id] = "asc";
                }

                if(!self.currentResultPage[experiment.id]) {
                    self.currentResultPage[experiment.id] = 1;
                }

                var offset = self.pageSize * (self.currentResultPage[experiment.id] - 1);
                var sortBy = "";
                var query = "";

                if(!self.resultTotalItems[experiment.id]) {
                    var optKey = experiment.optimizationKey;
                    if(optKey) {
                        self.resultSortType[experiment.id] = optKey;
                        if(experiment.direction === 'MAX') {
                            self.resultsReverse[experiment.id] = true;
                            self.resultOrderBy[experiment.id] = 'desc';
                        } else if(experiment.direction === 'MIN') {
                            self.resultsReverse[experiment.id] = false;
                            self.resultOrderBy[experiment.id] = 'asc';
                        }
                        sortBy = ';sort_by=' + self.resultSortType[experiment.id] + ':' + self.resultOrderBy[experiment.id] + ')';
                    } else {
                        sortBy = ')'
                    }
                } else {
                    if(self.resultSortType[experiment.id] && self.resultOrderBy[experiment.id]) {
                        sortBy = ';sort_by=' + self.resultSortType[experiment.id] + ':' + self.resultOrderBy[experiment.id] + ')';
                    } else {
                        sortBy = ')'
                    }
                }

                return "?expand=results(offset=" + offset + ";limit=" + self.pageSize + sortBy
            };
        }
    ]);