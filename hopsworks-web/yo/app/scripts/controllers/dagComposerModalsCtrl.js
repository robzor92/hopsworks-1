/*
 * This file is part of Hopsworks
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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
angular.module('hopsWorksApp')
    .controller('DagComposerModalsCtrl', ['$uibModalInstance', 'growl', 'operator', 'jobs', 'addedOperators',
        function($uibModalInstance, growl, operator, jobs, addedOperators) {
            self = this;
            self.operator = operator;
            self.jobs = jobs;
            self.addedOperators = addedOperators;

            self.tmpDagDefinition = {
                scheduleInterval: "@once",
                dependsOn: []
            };

            self.showAdvanced = false;
            self.toggleAdvanced = function() {
                self.showAdvanced = !self.showAdvanced;
            }

            self.finalizeDagDefition = function() {
                if (self.isUndefined(self.tmpDagDefinition.name) || self.tmpDagDefinition.name == "") {
                    var errorMsg = "DAG name is required";
                } else if (self.isUndefined(self.tmpDagDefinition.scheduleInterval) || self.tmpDagDefinition.scheduleInterval == "") {
                    var errorMsg = "DAG schedule interval is required";
                }
                if (self.isUndefined(errorMsg)) {
                    growl.info("You can continue adding Operators",
                        {title: "Created new DAG definition", ttl: 3000, referenceId: 'dag_comp_growl'});
                    $uibModalInstance.close(self.tmpDagDefinition);
                } else {
                    growl.error(errorMsg,
                        {title: 'Failed to create new DAG definition', ttl: 5000, referenceId: 'dag_comp_growl'});
                    $uibModalInstance.dismiss('cancel');
                }
            }

            self.addOperator2Dag = function() {
                if (self.isUndefined(self.operator.id) || self.operator.id == "") {
                    var errorMsg = "Operator name is required";
                } else if (self.operator.hasJobName
                    && (self.isUndefined(self.operator.jobName) || self.operator.jobName == "")) {
                        var errorMsg = "Job name to operate is required";
                } else if (self.operator.hasWait2Finish
                    && self.isUndefined(self.operator.wait)) {
                        var errorMsg = "Wait for job to finish flag is required";
                }
                if (self.isUndefined(errorMsg)) {
                    growl.info("Operator " + self.operator.id + " successfully added to DAG",
                        {title: "Added operator", ttl: 2000, referenceId: "dag_comp_growl"});
                    self.operator.dependsOn = [];
                    for (var idx = 0; idx < self.tmpDagDefinition.dependsOn.length; idx++) {
                        self.operator.dependsOn[idx] = self.tmpDagDefinition.dependsOn[idx].id;
                    }
                    $uibModalInstance.close(self.operator);
                } else {
                    growl.error(errorMsg,
                        {title: "Failed to add operator", ttl: 5000, referenceId: "dag_comp_growl"});
                        $uibModalInstance.dismiss('cancel');
                }
            }

            self.isUndefined = function(input) {
                return typeof input === "undefined";
            }

            self.close = function() {
                $uibModalInstance.dismiss('cancel');
            }
        }
    ]);