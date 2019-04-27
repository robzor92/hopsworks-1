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
  describe 'experiment' do
    context 'without authentication' do
      before :all do
        with_valid_project
        reset_session
      end
      it "should fail" do
        get_results(@project[:id], "app_id_4252123_1", nil)
        expect_json(errorCode: 200003)
        expect_status(401)
      end
    end
    context 'with authentication get' do
      before :all do
        with_valid_tour_project("deep_learning")
      end
      after :all do
        clean_jobs(@project[:id])
      end
      it "should run experiment and produce results" do
        create_results_experiment_job(@project, experiment_1)
        run_experiment_blocking(experiment_1)
        get_experiments(@project[:id], nil)
        expect_status(200)
        experiment = json_body[:items][0]
        results = experiment[:results]
        expect(results[:combinations].count).to eq 6
        expect(results[:count]).to eq 6
        expect(URI(experiment[:href]).path).to eq "#{ENV['HOPSWORKS_API']}/project/#{@project[:id]}/experiments/#{experiment[:id]}/results"
      end
      it "should check combinations" do
        get_experiments(@project[:id], nil)
        expect_status(200)
        combinations = json_body[:items][0][:results][:combinations][0]
        expect(results[:combinations][:metrics].count).to eq 2
        expect(results[:combinations][:hyperparameters].count).to eq 2
      end
    end
  end
end
