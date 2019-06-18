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
    .controller('experimentDetailCtrl', ['$uibModalInstance', '$scope', 'ProjectService', 'ModalService',
        'growl', 'projectId', 'experiment',
        function ($uibModalInstance, $scope, ProjectService, ModalService, growl, projectId, experiment) {

            /**
             * Initialize controller state
             */
            var self = this;
            self.experiment = experiment;

            self.imagesTypes = ['.png', '.jpg', '.jpeg', '.tiff', '.gif', '.exif', '.bmp', '.bpg'];


            $scope.sortBy = function(sortType) {

                if($scope.sortType === sortType) {
                console.log('reverse')
                   $scope.reverse = !$scope.reverse;
                } else {
                  scope.reverse = false;
                }
                $scope.sortType = sortType;
            };

            self.viewImage = function(filePath) {
                ModalService.filePreview('lgg', filePath.replace(/^.*[\\\/]/, ''), filePath, self.projectId, "head").then(
                        function (success) {

                        }, function (error) {
                });
            };


            self.countKeys = function(obj) {

                           var count=0;
                           for(var prop in obj) {
                              if (obj.hasOwnProperty(prop)) {
                                 ++count;
                              }
                           }
                           return count;
            };

            self.hp_headers = [];

            if(experiment['results'] && self.countKeys(experiment['results']) > 0) {
            var results = experiment['results']['results'];

            if(results[0]['hyperparameters']) {
            var hyperparameters = results[0]['hyperparameters'];
            for (var z = 0; z < hyperparameters.length; z++) {
              self.hp_headers.push(hyperparameters[z].key)
            }
            }

            self.metric_headers = [];
            var metrics = results[0]['metrics'];
            for (var x = 0; x < metrics.length; x++) {
              self.metric_headers.push(metrics[x].key);
            }

            $scope.sortType = self.experiment.optimizationKey;

            self.all_headers = self.hp_headers.concat(self.metric_headers);

            self.experiments = []
            for (var i = 0; i < results.length; i++) {
             var tmp = []
             if(results[i]['hyperparameters']) {
             for (var y = 0; y < results[i]['hyperparameters'].length; y++) {
               tmp.push({'data': results[i]['hyperparameters'][y].value, 'image': false})
             }
             }
             if(results[i]['metrics']) {
             for (var y = 0; y < results[i]['metrics'].length; y++) {
               var is_file = String(results[i]['metrics'][y].value).indexOf('/') > -1;
               tmp.push({'data': results[i]['metrics'][y].value, 'image': is_file})
             }
             }
              self.experiments.push({'row': tmp})
            }
            }

            self.projectId = projectId;
            /**
             * Closes the modal
             */
            self.close = function () {
                $uibModalInstance.dismiss('cancel');
            };
        }]);

