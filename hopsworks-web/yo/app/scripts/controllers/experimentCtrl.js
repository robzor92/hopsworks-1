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
 * Controller for the job UI dialog.
 */
angular.module('hopsWorksApp')
    .controller('ExperimentCtrl', ['$scope', '$timeout', 'growl', '$location', 'ModalService', 'ProjectService', 'ExperimentService', 'TensorBoardService', '$interval',
        '$routeParams', '$route', '$sce', '$window',
        function($scope, $timeout, growl, $location, ModalService, ProjectService, ExperimentService, TensorBoardService, $interval,
            $routeParams, $route, $sce, $window) {

            var self = this;

            $scope.pageSize = 10;
            $scope.sortKey = 'start';
            $scope.reverse = true;
            self.projectId = $routeParams.projectID;

            self.experiments = []

            self.loading = false;
            self.loadingText = "";

            var startLoading = function(label) {
                self.loading = true;
                self.loadingText = label;
            };
            var stopLoading = function() {
                self.loading = false;
                self.loadingText = "";
            };

            $scope.sortBy = function(sortType) {
                console.log(sortType)
                $scope.reverse = ($scope.sortType === sortType) ? !$scope.reverse : false;
                $scope.sortType = sortType;
            };

            $scope.sortType = 'Start'

            self.deleteExperiment = function (id) {
                startLoading("Deleting Experiment...");
                ExperimentService.deleteExperiment(self.projectId, id).then(
                    function(success) {
                          stopLoading();
                          for(var i = 0; self.experiments.length > i; i++) {
                            if(self.experiments[i].id === id) {
                               console.log('removed exp')
                               self.experiments.splice(i, 1);
                               return;
                            }
                          }
                    },
                    function(error) {
                        stopLoading();
                        if (typeof error.data.usrMsg !== 'undefined') {
                            growl.error(error.data.usrMsg, {title: error.data.errorMsg, ttl: 8000});
                        } else {
                            growl.error("", {title: error.data.errorMsg, ttl: 8000});
                        }
                    });

            };

            self.viewExperiment = function (experiment) {
                ModalService.viewExperimentInfo('lg', self.projectId, experiment).then(
                function (success) {
                    self.getAll();
                }, function (error) {
                    self.getAll();
                });
            };

            self.getAll = function() {
                startLoading("Fetching Experiments...");
                ExperimentService.getAll(self.projectId).then(
                    function(success) {
                        stopLoading();
                        self.experiments = success.data.items;
                        console.log(self.experiments)
                    },
                    function(error) {
                        stopLoading();
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
                $location.path('project/' + self.projectId + '/datasets/Experiments/' + experiment_id);
            };
            self.getAll();
        }
    ]);