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
    .controller('experimentDetailCtrl', ['$uibModalInstance', '$scope', 'ProjectService',
        'growl', 'projectId', 'experiment',
        function ($uibModalInstance, $scope, ProjectService, growl, projectId, experiment) {

            /**
             * Initialize controller state
             */
            var self = this;
            self.experiment = experiment;

            $scope.sortType = 'metric';

            $scope.sortBy = function(sortType) {
                console.log(sortType)
                $scope.reverse = ($scope.sortType === sortType) ? !$scope.reverse : false;
                $scope.sortType = sortType;
            };

            self.hp_headers = [];
            var results = experiment['results'];
            var hyperparameters = results[0]['hyperparameters'];
            for (var i = 0; i < hyperparameters.length; i++) {
              self.hp_headers.push(hyperparameters[i].key)
            }

            console.log(results)

            self.values = []
            for (var i = 0; i < results.length; i++) {
            console.log('outer')
             var tmp = []
             for (var y = 0; y < results[i]['hyperparameters'].length; y++) {
               tmp.push(results[i]['hyperparameters'][y].value)
             }
              tmp.push(results[i]['metrics'][0].value)
              self.values.push({'val': tmp})
            }

            self.values.sort(function(a,b) {
                    return a['val'][self.hp_headers.length] - b['val'][self.hp_headers.length];
                });

            console.log(self.values)



            self.projectId = projectId;
            /**
             * Closes the modal
             */
            self.close = function () {
                $uibModalInstance.dismiss('cancel');
            };

        }]);

