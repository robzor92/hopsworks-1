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
    .controller('ExperimentCtrl', ['$scope', '$timeout', 'growl', 'ProjectService','ExperimentService', 'TensorBoardService', '$interval',
        '$routeParams', '$route', '$sce', '$window',
        function($scope, $timeout, growl, ProjectService, ExperimentService, TensorBoardService, $interval,
            $routeParams, $route, $sce, $window) {

            var self = this;

            $scope.pageSize = 10;
            $scope.sortKey = 'start';
            $scope.reverse = true;
            self.projectId = $routeParams.projectID;

            self.experiments = [{'id': 'app_id_3232','name': 'resnet20', 'state': 'running', 'user': 'derp', 'start': 'tolo', 'end': 'dwad'},
            {'id': 'app_id_1337','name': 'resnet50', 'state': 'running', 'user': 'derp', 'start': 'tolo', 'end': 'dwad'}]

            self.showProvenance = function (app_id) {
                ModalService.viewTrainingDatasetDependencies('lg', self.projectId, trainingDataset).then(
                function (success) {
                    self.showTrainingDatasets()
                }, function (error) {
                    self.showTrainingDatasets()
                });
            };

            self.getAll = function() {
                ExperimentService.getAll(self.projectId).then(
                    function(success) {
                        self.experiments = success.data;
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
            self.getAll();
        }
    ]);