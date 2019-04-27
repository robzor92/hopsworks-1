=begin
 This file is part of Hopsworks
 Copyright (C) 2018, Logical Clocks AB. All rights reserved

 Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 the GNU Affero General Public License as published by the Free Software Foundation,
 either version 3 of the License, or (at your option) any later version.

 Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 PURPOSE.  See the GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/>.
=end
describe "On #{ENV['OS']}" do
  experiment_1 = "experiment_1"
  experiment_2 = "experiment_2"
  describe 'experiment' do
    context 'without authentication' do
      before :all do
        with_valid_project
        reset_session
      end
      it "should fail" do
        get_tensorboard(@project[:id], "app_id_4252123_1")
        expect_json(errorCode: 200003)
        expect_status(401)
      end
    end
    context 'with authentication create, delete, get' do
      before :all do
        with_valid_tour_project("deep_learning")
      end
      after :each do
        clean_jobs(@project[:id])
      end
      it "should run experiment and create a tensorboard" do
       create_experiment_job(@project, experiment_1)
       run_experiment_blocking(experiment_1)

       get_experiments(@project[:id], nil)
       ml_id = json_body[:items][0][:id]

       get_tensorboard(@project[:id], ml_id)
       expect_status(404)

       create_tensorboard(@project[:id], ml_id)
       expect_status(201)

       get_tensorboard(@project[:id], ml_id)
       expect_status(200)

       expect(URI(experiment[:href]).path).to eq "#{ENV['HOPSWORKS_API']}/project/#{@project[:id]}/experiments/#{experiment[:id]}/tensorboard"
      end
      it "should run experiment and delete a tensorboard" do
       create_experiment_job(@project, experiment_2)
       run_experiment_blocking(experiment_2)

       get_experiments(@project[:id], nil)
       ml_id = json_body[:items][0][:id]

       create_tensorboard(@project[:id], ml_id)
       expect_status(201)

       get_tensorboard(@project[:id], ml_id)
       expect_status(200)

       delete_tensorboard(@project[:id], ml_id)
       expect_status(204)

       get_tensorboard(@project[:id], ml_id)
       expect_status(404)
      end
    end
  end
end
