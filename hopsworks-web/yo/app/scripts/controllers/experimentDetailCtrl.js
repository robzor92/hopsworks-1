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
 * Controller for the Featuregroup-Info view
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

            $scope.sortType = self.experiment.optimizationKey;

            self.showProvenanceView = false;
            self.showResultsView = true;

            self.provenanceLoading = false;
            self.resultsLoading = false;

            self.modelLink = null;

            self.query = "";

            self.buildQuery = function() {
                self.query = "";
                if (self.showProvenanceView === true && self.showResultsView === true) {
                    self.query = "?expand=provenance&expand=results"
                } else if (self.showProvenanceView === true && self.showResultsView === false) {
                    self.query = "?expand=provenance"
                } else if (self.showProvenanceView === false && self.showResultsView === true) {
                    self.query = "?expand=results"
                }
            };


            $scope.sortBy = function(sortType) {

                if ($scope.sortType === sortType) {

                    $scope.reverse = !$scope.reverse;
                } else {
                    scope.reverse = false;
                }
                $scope.sortType = sortType;
            };

            self.viewImage = function(filePath) {
                ModalService.filePreview('lgg', filePath.replace(/^.*[\\\/]/, ''), filePath, self.projectId, "head").then(
                    function(success) {

                    },
                    function(error) {});
            };

            self.showProvenance = function() {
            try {
                    self.provenanceLoading = true;
                    self.showProvenanceView = !self.showProvenanceView;
                    self.getExperiment();
                } catch (error) {
                } finally {
                    self.provenanceLoading = false;
                }
            };

            self.showResults = function() {
            try {
                    self.resultsLoading = true;
                    self.showResultsView = !self.showResultsView;
                    self.getExperiment();
                } catch (error) {
                } finally {
                    self.resultsLoading = false;
                }
            };

            self.getExperiment = function() {
                self.buildQuery();
                self.buildModelLink();
                if (self.showResultsView || self.showProvenanceView) {
                    ExperimentService.get(self.projectId, self.experiment.id, self.query).then(
                        function(success) {
                            self.experiment = success.data;
                            self.buildModelLink();
                            self.initResultsTable();
                            if(self.experiment.optimizationKey) {
                                $scope.sortType = self.experiment.optimizationKey;
                            } else {
                                $scope.sortType = 'metric';
                            }
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
                }
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
                    var results = self.experiment['results']['results'];


                    if (results) {
                        if (results[0]['hyperparameters']) {

                            var hyperparameters = results[0]['hyperparameters'];
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
                            if (results[i]['hyperparameters']) {
                                for (var y = 0; y < results[i]['hyperparameters'].length; y++) {
                                    tmp.push({
                                        'data': results[i]['hyperparameters'][y].value,
                                        'image': false
                                    })
                                }
                            }
                            if (results[i]['metrics']) {
                                for (var y = 0; y < results[i]['metrics'].length; y++) {
                                    var is_file = String(results[i]['metrics'][y].value).indexOf('/') > -1;
                                    tmp.push({
                                        'data': results[i]['metrics'][y].value,
                                        'image': is_file
                                    });
                                }
                            }
                            self.experiments.push({
                                'row': tmp
                            });
                        }

                        try {
                            console.log('gogo')
                            for(var optIndex = 0; optIndex < self.all_headers.length; optIndex++) {
                            console.log('loop')
                            console.log(self.all_headers[optIndex])
                            console.log($scope.sortType)
                                if(self.all_headers[optIndex] === $scope.sortType) {
                                    if (self.experiment.direction === 'min') {
                                        console.log('sorting')
                                        self.experiments.sort(function(a, b) {
                                            return (parseFloat(a.row[optIndex].data) - parseFloat(b.row[optIndex].data))
                                        });
                                    } else if (self.experiment.direction === 'max') {
                                        console.log('sorting')
                                        self.experiments.sort(function(a, b) {
                                            return (parseFloat(b.row[optIndex].data) - parseFloat(a.row[optIndex].data))
                                        });
                                    }
                                }
                            }
                        } catch (error) {
                            console.error(error);
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

            self.goToModel = function (path) {
                self.close();
                $location.path('project/' + self.projectId + '/datasets/' + path);
            };

            self.buildQuery();
            self.getExperiment();
            /**
             * Closes the modal
             */
            self.close = function() {
                $uibModalInstance.dismiss('cancel');
            };
        }
    ]);