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

/**
 * Controller for the experimentDetailed view
 */
angular.module('hopsWorksApp')
    .controller('experimentDetailCtrl', ['$uibModalInstance', '$scope', '$location', 'ProjectService', 'ExperimentService', 'ModalService',
        'growl', 'projectId', 'experiment',
        function($uibModalInstance, $scope, $location, ProjectService, ExperimentService, ModalService, growl, projectId, experiment) {

            /**
             * Initialize controller state
             */
            var self = this;

            self.projectId = projectId;

            self.experiment = experiment;

            self.sortType = self.experiment.optimizationKey;

            if(self.experiment.optimizationKey) {
                if(self.experiment.direction === 'max') {
                    self.orderBy = 'desc';
                    self.reverse = true;
                } else if(self.experiment.direction === 'min') {
                    self.orderBy = 'asc';
                    self.reverse = false;
                }
            }

            self.showProvenanceView = false;
            self.showResultsView = false;

            self.loading = false;
            self.loadingText = "";

            self.modelLink = null;

            self.firstLoading = true;

            self.pageSize = 12;
            self.currentPage = 1;
            self.totalItems = 0;

            self.query = "";

            self.experimentsSubset = [];

            var startLoading = function(label) {
                if(label) {
                    self.loading = true;
                    self.loadingText = label;
                };
            };
            var stopLoading = function() {
                self.loading = false;
                self.loadingText = "";
            };

            self.buildQuery = function() {
                var offset = self.pageSize * (self.currentPage - 1);
                var sortBy = "";
                self.query = "";

                if(self.sortType && self.orderBy) {
                    sortBy = ';sort_by=' + self.sortType + ':' + self.orderBy + ')';
                } else {
                    sortBy = ')';
                }

                if(self.firstLoading) {
                    self.query = "?expand=results(offset=" + offset + ";limit=" + self.pageSize + sortBy
                } else if (self.showProvenanceView === true && self.showResultsView === true) {
                    self.query = "?expand=provenance&expand=results(offset=" + offset + ";limit=" + self.pageSize + sortBy
                } else if (self.showProvenanceView === true && self.showResultsView === false) {
                    self.query = "?expand=provenance"
                } else if (self.showProvenanceView === false && self.showResultsView === true) {
                    self.query = "?expand=results(offset=" + offset + ";limit=" + self.pageSize + sortBy
                };
            };

            self.order = function () {
                if (self.reverse) {
                    self.orderBy = "desc";
                } else {
                    self.orderBy = "asc";
                }
            };

            self.sortBy = function(type) {
                if(self.sortType !== type) {
                    self.reverse = true;
                } else {
                    self.reverse = !self.reverse; //if true make it false and vice versa
                }
                self.sortType = type;
                self.order();
                self.getExperiment(null);
            };

            self.viewImage = function(filePath) {
                ModalService.filePreview('lgg', filePath.replace(/^.*[\\\/]/, ''), filePath, self.projectId, "head").then(
                    function(success) {

                    },
                    function(error) {});
            };

            self.showProvenance = function() {
                self.showProvenanceView = !self.showProvenanceView;
                if(self.showProvenanceView) {
                    self.getExperiment('Fetching provenance information');
                }
            };

            self.showResults = function() {
                self.showResultsView = !self.showResultsView;
                if(self.showResultsView) {
                    self.getExperiment('Fetching experiment results');
                }
            };

            self.getExperiment = function(loadingText) {
                self.buildQuery();
                self.buildModelLink();
                    startLoading(loadingText);
                    ExperimentService.get(self.projectId, self.experiment.id, self.query).then(
                        function(success) {
                            if(self.firstLoading) {
                                self.showResultsView = true;
                                self.firstLoading = false;
                            }
                            stopLoading();
                            self.experiment = success.data;
                            self.buildModelLink();
                            self.initResultsTable();
                            if(self.experiment.results.combinations) {
                                self.totalItems = self.experiment.results.count;
                            }
                        },
                        function(error) {
                            stopLoading();
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
            };

            self.countKeys = function(obj) {
                var count = 0;
                for (var prop in obj) {
                    if (obj.hasOwnProperty(prop)) {
                        ++count;
                    }
                }
                return count;
            };

            self.hp_headers = [];
            self.metric_headers = [];
            self.all_headers = [];
            self.experiments = [];

            self.initResultsTable = function() {

                self.hp_headers = [];
                self.metric_headers = [];
                self.all_headers = [];
                self.experiments = [];

                if (self.experiment['results'] && self.countKeys(self.experiment['results']) > 0) {
                    var results = self.experiment['results']['combinations'];
                    if (results) {
                        if (results[0]['parameters']) {

                            var hyperparameters = results[0]['parameters'];
                            for (var z = 0; z < hyperparameters.length; z++) {
                                self.hp_headers.push(hyperparameters[z].key)
                            }
                        }

                        var metrics = results[0]['metrics'];
                        for (var x = 0; x < metrics.length; x++) {
                            self.metric_headers.push(metrics[x].key);
                        }

                        self.all_headers = self.hp_headers.concat(self.metric_headers);

                        for (var i = 0; i < results.length; i++) {
                            var tmp = []
                            if (results[i]['parameters']) {
                                for (var y = 0; y < results[i]['parameters'].length; y++) {
                                    tmp.push({
                                        'data': results[i]['parameters'][y].value,
                                        'file': false
                                    })
                                }
                            }
                            if (results[i]['metrics']) {
                                for (var y = 0; y < results[i]['metrics'].length; y++) {
                                    var is_file = String(results[i]['metrics'][y].value).indexOf('/') > -1;
                                    tmp.push({
                                        'data': results[i]['metrics'][y].value,
                                        'file': is_file
                                    });
                                }
                            }
                            self.experiments.push({
                                'row': tmp
                            });
                        }

                    }
                }
            };

            self.buildModelLink = function() {
                if(self.experiment.model) {
                    var modelSplit = self.experiment.model.split('_');
                    var modelName = modelSplit[0];
                    var modelVersion = modelSplit[1];
                    self.modelLink = 'Models/' + modelName + '/' + modelVersion;
                }
            };

            self.getNewPage = function() {
                self.getExperiment(null);
            };

            self.goToDirectory = function (path) {
                self.close();
                $location.path('project/' + self.projectId + '/datasets/' + path);
            };

            self.buildQuery();
            self.getExperiment('Loading detailed information');
            /**
             * Closes the modal
             */
            self.close = function() {
                $uibModalInstance.dismiss('cancel');
            };
        }
    ]);