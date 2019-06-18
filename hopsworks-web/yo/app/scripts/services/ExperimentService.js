
'use strict';

angular.module('hopsWorksApp')
        .factory('ExperimentService', ['$http', function ($http) {
            return {
              getAll: function (projectId) {
                return $http.get('/api/project/' + projectId + '/experiments');
              }
            };
          }]);
