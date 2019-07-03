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

require 'pp'

describe "On #{ENV['OS']}" do
  before :all do
    $stdout.sync = true
    with_valid_session
    @project1_name = "prov_proj_#{short_random_id}"
    @project2_name = "prov_proj_#{short_random_id}"
    @app1_id = "application_#{short_random_id}_0001"
    @app2_id = "application_#{short_random_id}_0001"
    @app3_id = "application_#{short_random_id}_0001"
    @experiment_app1_name1 = "#{@app1_id}_1"
    @experiment_app2_name1 = "#{@app2_id}_1"
    @experiment_app3_name1 = "#{@app3_id}_1"
    @experiment_app1_name2 = "#{@app1_id}_2"
    @not_experiment_name = "not_experiment"
    @model1_name = "model_a"
    @model2_name = "model_b"
    @model_version1 = "1"
    @model_version2 = "2"
    pp "create project: #{@project1_name}"
    @project1 = create_project_by_name(@project1_name)
    pp "create project: #{@project2_name}"
    @project2 = create_project_by_name(@project2_name)
  end

  after :all do 
    pp "delete projects"
    delete_project(@project1)
    delete_project(@project2)
  end

  def create_dir(project, dirname) 
    target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset"
    payload_string = '{"name": "' + dirname + '"}'
    payload = JSON.parse(payload_string)
    post target, payload
    expect_status(200)
  end

  def delete_dir(project, dirname) 
    target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset/file/#{dirname}"
    delete target
    expect_status(200)
  end

  def get_ml_asset_in_project(project, ml_type, withAppState) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/mlType/#{ml_type}/list"
    query_params = "?withAppState=#{withAppState}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_ml_asset_by_id(project, ml_type, ml_id, withAppState, status) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/mlType/#{ml_type}/exact"
    query_params = "?mlId=#{ml_id}&withAppState=#{withAppState}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(status)
    result
  end

  def add_xattr(original, xattr_name, xattr_value, xattr_op, increment)
    xattrRecord = original.dup
    xattrRecord["inode_operation"] = xattr_op
    xattrRecord["io_logical_time"] = original["io_logical_time"]+increment
    xattrRecord["io_timestamp"] = original["io_timestamp"]+increment
    xattrRecord["i_xattr_name"] = xattr_name
    xattrRecord["io_logical_time_batch"] = original["io_logical_time_batch"]+increment
    xattrRecord["io_timestamp_batch"] = original["io_timestamp_batch"]+increment
    xattrRecord.save!

    FileProvXAttr.create(inode_id: xattrRecord["inode_id"], namespace: 5, name: xattr_name, inode_logical_time: xattrRecord["io_logical_time"], value: xattr_value)
  end

  def add_app_states1(app_id, user)
    timestamp = Time.now
    AppProv.create(id: app_id, state: "null", timestamp: timestamp, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "null", timestamp: timestamp+5, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
  end
  def add_app_states2(app_id, user)
    timestamp = Time.now
    AppProv.create(id: app_id, state: "null", timestamp: timestamp, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "null", timestamp: timestamp+5, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "FINISHED", timestamp: timestamp+10, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: timestamp+50)
  end

  def wait_for_epipe() 
    pp "waiting"
    sleepCounter1 = 0
    sleepCounter2 = 0
    until FileProv.all.empty? || sleepCounter1 == 10 do
      sleep(5)
      sleepCounter1 += 1
    end
    until AppProv.all.empty? || sleepCounter2 == 10 do
      sleep(5)
      sleepCounter2 += 1
    end
    sleep(5)
    expect(sleepCounter1).to be < 10
    expect(sleepCounter2).to be < 10
    pp "done waiting"
  end
  
  describe 'provenance tests - experiments' do
    def create_experiment(project, experiment_name) 
      pp "create experiment #{experiment_name} in project #{project[:inode_name]}"
      create_dir(project, "Experiments/#{experiment_name}")
    end
    
    def delete_experiment(project, experiment_name) 
      pp "delete experiment #{experiment_name} in project #{project[:inode_name]}"
      delete_dir(project, "Experiments/#{experiment_name}")
    end
    
    def experiment_ml_id(experiment_name)
      "#{experiment_name}"
    end

    def check_experiment1(experiments, experiment_id) 
      experiment = experiments.select { |e| e["mlId"] == experiment_id }
      expect(experiment.length).to eq 1
    end

    def check_experiment2(experiments, experiment_id, xattrs) 
      experiment = experiments.select { |e| e["mlId"] == experiment_id }
      expect(experiment.length).to eq 1
      #pp experiment
      expect(experiment[0]["xattrs"]["entry"].length).to eq xattrs.length
      xattrs.each do |key, value|
        # pp experiment[0]["xattrs"]["entry"]
        xattr = experiment[0]["xattrs"]["entry"].select do |e| 
          e["key"] == key && e["value"] == value
        end
        expect(xattr.length).to eq 1
        #pp xattr
      end
    end

    def check_experiment3(experiments, experiment_id, currentState) 
      experiment = experiments.select { |e| e["mlId"] == experiment_id }
      expect(experiment.length).to eq 1

      pp experiment[0]["appState"]
      expect(experiment[0]["appState"]["currentState"]).to eq currentState
    end

    describe 'experiment with app states' do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with app states" do
        create_experiment(@project1, @experiment_app1_name1)
        create_experiment(@project1, @experiment_app1_name2)
        create_experiment(@project1, @experiment_app2_name1)
        experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name1)
        user_name = experiment_record[0]["io_user_name"]
        expect(experiment_record.length).to eq 1
        add_app_states1(@app1_id, user_name)
        add_app_states2(@app2_id, user_name)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment" do 
        wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", true)
        expect(result1.length).to eq 2
        check_experiment3(result1, experiment_ml_id(@experiment_app1_name1), "RUNNING")
        check_experiment3(result1, experiment_ml_id(@experiment_app1_name2), "RUNNING")
        check_experiment3(result1, experiment_ml_id(@experiment_app2_name1), "FINISHED")
      end

      it "delete experiments" do
        delete_experiment(@project1, @experiment_app1_name1)
        delete_experiment(@project1, @experiment_app1_name2)
        delete_experiment(@project1, @experiment_app2_name1)
      end

      it "check experiments" do 
        wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", true)
        expect(result1.length).to eq 0
      end
    end
  end

  describe 'provenance tests - models' do
    def create_model1(project, model_name) 
      pp "create model #{model_name} in project #{project[:inode_name]}"
      create_dir(project, "Models/#{model_name}")
    end

    def create_model2(project, model_name, model_version) 
      pp "create model #{model_name}_#{model_version} in project #{project[:inode_name]}"
      create_dir(project, "Models/#{model_name}/#{model_version}")
    end
    
    def delete_model(project, model_name) 
      pp "delete model #{model_name} in project #{project[:inode_name]}"
      delete_dir(project, "Models/#{model_name}")
    end
    
    def model_ml_id(model_name, model_version)
      "#{model_name}_#{model_version}"
    end

    def check_model(models, model_id, xattrs) 
      model = models.select {|m| m["mlId"] == model_id }
      expect(model.length).to eq 1
      #pp model
      expect(model[0]["xattrs"]["entry"].length).to eq xattrs.length
      xattrs.each do |key, value|
        #pp model[0]["xattrs"]["entry"]
        xattr = model[0]["xattrs"]["entry"].select do |e| 
          e["key"] == key && e["value"] == value
        end
        expect(xattr.length).to eq 1
        #pp xattr
      end
    end
  end
end