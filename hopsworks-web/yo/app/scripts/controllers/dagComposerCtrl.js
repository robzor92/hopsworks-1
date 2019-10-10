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
    .controller('DagComposerCtrl', ['$routeParams', 'growl', '$location', 'ModalService', 'AirflowService',
        'JobService',
    function($routeParams, growl, $location, ModalService, AirflowService, JobService) {
        self = this;
        self.projectId = $routeParams.projectID;

        self.jobs;
        self.newDagDefined = false;
        // DAG properties
        self.dag = {
            // Fill it in backend
            projectId: self.projectId,
            name: "",
            // Fill it in backend
            owner: "meb10000",
            scheduleInterval: "@once",
            operators: []
        };
        var getJobsPromise;

        self.showCart = true;
        self.toggleCart = function() {
            self.showCart = !self.showCart;
        }

        self.removeOperatorFromDag = function(index) {
            self.dag.operators.splice(index, 1);
        }

        self.init = function() {
            self.getJobsPromise = JobService.getJobs(self.projectId, 0, 0, "");
        }

        self.isUndefined = function(input) {
            return typeof input === "undefined";
        }

        // Operators
        var Operator = function(name, description, hasJobName, hasWait2Finish) {
            this.name = name;
            this.description = description;
            this.hasJobName = hasJobName;
            this.hasWait2Finish = hasWait2Finish;
        }

        var launchJobOperator = new Operator("HopsworksLaunchOperator",
            "Operator to launch a Job in Hopsworks. Job should already be defined in Jobs UI "
                + "and job name in operator must match the job name in Jobs UI.",
            true, true);
        
        var jobSensor = new Operator("HopsworksJobSuccessSensor",
            "Operator which waits for the completion of a specific job. Job must be defined "
                + "in Jobs UI and job name in operator must match the job name in Jobs UI. "
                + "The task will fail too if the job which is waiting for fails.",
            true, false);

        self.availableOperators = [launchJobOperator, jobSensor];

        self.defineNewDag = function() {
            var thisthis = self;
            ModalService.defineNewAirflowDag('lg').then(
                function(dag) {
                    thisthis.dag.name = dag.name;
                    thisthis.dag.scheduleInterval = dag.scheduleInterval;
                    thisthis.dag.apiKey = dag.apiKey;
                    console.log("New DAG " + thisthis.dag);
                    thisthis.newDagDefined = true;
                    self = thisthis;
                }, function(error) {
                    thisthis.newDagDefined = false;
                    self = thisthis;
                }
            )
        }

        self.addOperator = function(operator) {
            console.log("Adding new operator");
            if (!self.newDagDefined) {
                growl.error("You should define DAG properties before start adding operators",
                    {title: "Failed to add operator", ttl: 5000, referenceId: "dag_comp_growl"});
                return;
            }

            var thisthis = self;
            var newOperator = new Operator(operator.name, operator.description, operator.hasJobName, operator.hasWait2Finish);
            if (operator.hasWait2Finish) {
                newOperator.wait = false;
            }
            self.getJobsPromise.then(
                function(success) {
                    if (self.isUndefined(self.jobs)) {
                        var idx = 0;
                        self.jobs = [];
                        success.data.items.forEach(function(value, key) {
                            self.jobs[idx] = value.name;
                            idx++;
                        })
                    }
                    ModalService.addOperator2AirflowDag('lg', newOperator, self.jobs, self.dag.operators).then(
                        function(operator) {
                            thisthis.dag.operators.push(operator);
                            self = thisthis;
                        }, function(error) {
                            self = thisthis;
                        }
                    )
                }, function(error) {
                    growl.error("Could not fetch Project's jobs. Please try again",
                        {title: "Failed to add operator", ttl: 5000, referenceId: "dag_comp_growl"});
                }
            )
        }

        self.generateAirflowDag = function() {
            AirflowService.generateDag(self.projectId, self.dag).then(
                function(success) {
                    growl.info("Generated DAG " + self.dag.name,
                        {title: "Success", ttl: 3000, referenceId: "dag_comp_growl"});
                    $location.path("project/" + self.projectId + "/airflow");
                }, function(error) {
                    growl.error(error.data.usrMsg,
                        {title: "Could not generate DAG file", ttl: 5000, referenceId: "dag_comp_growl"});
                }
            )
        }
        self.init();
    }
]);