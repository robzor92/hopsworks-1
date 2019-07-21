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
    pp "user email: #{@user["email"]}"
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
    @td1_name = "td_a"
    @td2_name = "td_b"
    @td_version1 = "1"
    @td_version2 = "2"
    @xattrV1 = JSON['{"f1_1":"v1","f1_2":{"f2_1":"val1"}}']
    @xattrV2 = JSON['{"f1_1":"v1","f1_2":{"f2_2":"val2"}}']
    @xattrV3 = JSON['[{"f3_1":"val1","f3_2":"val2"},{"f4_1":"val3","f4_2":"val4"}]']
    @xattrV4 = "notJson"
    @xattrV5 = JSON['[{"f3_1":"val1","f3_2":"val1"},{"f3_1":"val2","f3_2":"val2"}]']
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
  
  describe "test epipe" do
    it "restart epipe" do
      execute_remotely @hostname, "sudo systemctl restart epipe"
    end

    it "wait for epipe" do 
      prov_wait_for_epipe() 
    end
  end

  describe 'provenance tests - experiments' do
    describe 'simple experiments' do
      it "create experiments" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        prov_create_experiment(@project1, @experiment_app2_name1)
        prov_create_experiment(@project2, @experiment_app3_name1)
      end

      it "check experiments" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false)
        expect(result1.length).to eq 2
        prov_check_asset_with_id(result1, prov_experiment_id(@experiment_app1_name1))
        prov_check_asset_with_id(result1, prov_experiment_id(@experiment_app2_name1))

        result2 = get_ml_asset_in_project(@project2, "EXPERIMENT", false)
        expect(result2.length).to eq 1
        prov_check_asset_with_id(result2, prov_experiment_id(@experiment_app3_name1))
        
        result3 = get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false, 200)
      end

      it "delete experiments" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app2_name1)
        prov_delete_experiment(@project2, @experiment_app3_name1)
      end
      
      it "check experiments" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false)
        expect(result1.length).to eq 0

        result2 = get_ml_asset_in_project(@project2, "EXPERIMENT", false)
        expect(result2.length).to eq 0
        
        result3 = get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false, 404)
      end
    end

    describe 'experiment with xattr' do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experimentRecord = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name1)
        expect(experimentRecord.length).to eq 1
        prov_add_xattr(experimentRecord[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
        prov_add_xattr(experimentRecord[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false)
        expect(result1.length).to eq 1
        xattrs = Hash.new
        xattrs["xattr_key_1"] = "xattr_value_1"
        xattrs["xattr_key_2"] = "xattr_value_2"
        prov_check_asset_with_xattrs(result1, prov_experiment_id(@experiment_app1_name1), xattrs)
      end

      it "delete experiments" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
      end

      it "check experiments" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false)
        expect(result1.length).to eq 0
      end
    end

    describe 'experiment with xattr add, update and delete' do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experimentRecord = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name1)
        expect(experimentRecord.length).to eq 1
        prov_add_xattr(experimentRecord[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
        prov_add_xattr(experimentRecord[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)
        prov_add_xattr(experimentRecord[0], "xattr_key_3", "xattr_value_3", "XATTR_ADD", 3)
        prov_add_xattr(experimentRecord[0], "xattr_key_1", "xattr_value_1_updated", "XATTR_UPDATE", 4)
        prov_add_xattr(experimentRecord[0], "xattr_key_2", "", "XATTR_DELETE", 5)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false)
        expect(result1.length).to eq 1
        xattrs = Hash.new
        xattrs["xattr_key_1"] = "xattr_value_1_updated"
        xattrs["xattr_key_3"] = "xattr_value_3"
        prov_check_asset_with_xattrs(result1, prov_experiment_id(@experiment_app1_name1), xattrs)
      end

      it "delete experiments" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
      end

      it "check experiments" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false)
        expect(result1.length).to eq 0
      end
    end

    describe 'experiment with app states' do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with app states" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment1_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name1)
        expect(experiment1_record.length).to eq 1
        prov_add_xattr(experiment1_record[0], "appId", "#{@app1_id}", "XATTR_ADD", 1)

        prov_create_experiment(@project1, @experiment_app1_name2)
        experiment2_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name2)
        expect(experiment2_record.length).to eq 1
        prov_add_xattr(experiment2_record[0], "appId", "#{@app1_id}", "XATTR_ADD", 1)
        
        prov_create_experiment(@project1, @experiment_app2_name1)
        experiment3_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app2_name1)
        expect(experiment3_record.length).to eq 1
        prov_add_xattr(experiment3_record[0], "appId", "#{@app2_id}", "XATTR_ADD", 1)

        user_name = experiment1_record[0]["io_user_name"]
        prov_add_app_states1(@app1_id, user_name)
        prov_add_app_states2(@app2_id, user_name)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", true)
        expect(result1.length).to eq 3
        prov_check_experiment3(result1, prov_experiment_id(@experiment_app1_name1), "RUNNING")
        prov_check_experiment3(result1, prov_experiment_id(@experiment_app1_name2), "RUNNING")
        prov_check_experiment3(result1, prov_experiment_id(@experiment_app2_name1), "FINISHED")
      end

      it "delete experiments" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app1_name2)
        prov_delete_experiment(@project1, @experiment_app2_name1)
      end

      it "check experiments" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", true)
        expect(result1.length).to eq 0
      end
    end

    describe 'not experiment in Experiments' do
      it "create not experiment dir" do
        prov_create_experiment(@project1, @not_experiment_name)
      end
      it "check not experiment" do 
        prov_wait_for_epipe() 
        result = get_ml_asset_in_project(@project1, "EXPERIMENT", false)
        expect(result.length).to eq 0
      end

      it "delete not experiment" do
        prov_delete_experiment(@project1, @not_experiment_name)
      end
    end
  end

  describe 'provenance tests - models' do
    
    describe 'simple models' do
      it "create models" do
        prov_create_model(@project1, @model1_name)
        prov_create_model_version(@project1, @model1_name, @model_version1)
        prov_create_model_version(@project1, @model1_name, @model_version2)
        prov_create_model(@project1, @model2_name)
        prov_create_model_version(@project1, @model2_name, @model_version1)
        prov_create_model(@project2, @model1_name)
        prov_create_model_version(@project2, @model1_name, @model_version1)
      end

      it "check models" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL", false)
        expect(result1.length).to eq 3
        prov_check_asset_with_id(result1, prov_model_id(@model1_name, @model_version1))
        prov_check_asset_with_id(result1, prov_model_id(@model1_name, @model_version2))
        prov_check_asset_with_id(result1, prov_model_id(@model2_name, @model_version1))

        result2 = get_ml_asset_in_project(@project2, "MODEL", false)
        expect(result2.length).to eq 1
        prov_check_asset_with_id(result2, prov_model_id(@model1_name, @model_version1))
        
        result3 = get_ml_asset_by_id(@project1, "MODEL", prov_model_id(@model1_name, @model_version2), false, 200)
      end

      it "delete models" do
        prov_delete_model(@project1, @model1_name)
        prov_delete_model(@project1, @model2_name)
        prov_delete_model(@project2, @model1_name)
      end
      
      it "check models" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL", false)
        expect(result1.length).to eq 0

        result2 = get_ml_asset_in_project(@project2, "MODEL", false)
        expect(result2.length).to eq 0
        
        result3 = get_ml_asset_by_id(@project1, "MODEL", prov_model_id(@model1_name, @model_version2), false, 404)
      end
    end
    describe 'model with xattr' do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create model with xattr" do
        prov_create_model(@project1, @model1_name)
        prov_create_model_version(@project1, @model1_name, @model_version1)
        modelRecord = FileProv.where("project_name": @project1["inode_name"], "i_parent_name": @model1_name, "i_name": @model_version1)
        expect(modelRecord.length).to eq 1
        prov_add_xattr(modelRecord[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
        prov_add_xattr(modelRecord[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check model" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL", false)
        expect(result1.length).to eq 1
        xattrs = Hash.new
        xattrs["xattr_key_1"] = "xattr_value_1"
        xattrs["xattr_key_2"] = "xattr_value_2"
        prov_check_asset_with_xattrs(result1, prov_model_id(@model1_name, @model_version1), xattrs)
      end

      it "delete model" do
        prov_delete_model(@project1, @model1_name)
      end

      it "check models" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL", false)
        expect(result1.length).to eq 0
      end
    end
  end

  describe 'provenance tests - training datasets' do
    describe 'simple training datasets' do
      it "create training datasets" do
        prov_create_td(@project1, @td1_name, @td_version1)
        prov_create_td(@project1, @td1_name, @td_version2)
        prov_create_td(@project1, @td2_name, @td_version1)
        prov_create_td(@project2, @td1_name, @td_version1)
      end

      it "check training datasets" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
        expect(result1.length).to eq 3
        prov_check_asset_with_id(result1, prov_td_id(@td1_name, @td_version1))
        prov_check_asset_with_id(result1, prov_td_id(@td1_name, @td_version2))
        prov_check_asset_with_id(result1, prov_td_id(@td2_name, @td_version1))

        result2 = get_ml_asset_in_project(@project2, "TRAINING_DATASET", false)
        expect(result2.length).to eq 1
        prov_check_asset_with_id(result2, prov_td_id(@td1_name, @td_version1))
        
        result3 = get_ml_asset_by_id(@project1, "TRAINING_DATASET", prov_td_id(@td1_name, @td_version1), false, 200)
      end

      it "delete training datasets" do
        prov_delete_td(@project1, @td1_name, @td_version1)
        prov_delete_td(@project1, @td1_name, @td_version2)
        prov_delete_td(@project1, @td2_name, @td_version1)
        prov_delete_td(@project2, @td1_name, @td_version1)
      end
      
      it "check training datasets" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
        expect(result1.length).to eq 0

        result2 = get_ml_asset_in_project(@project2, "TRAINING_DATASET", false)
        expect(result2.length).to eq 0
        
        result3 = get_ml_asset_by_id(@project1, "TRAINING_DATASET", prov_td_id(@td1_name, @td_version1), false, 404)
      end
    end
    describe 'training dataset with xattr' do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create training dataset with xattr" do
        prov_create_td(@project1, @td1_name, @td_version1)
        td_record = prov_get_td_record(@project1, @td1_name, @td_version1)
        expect(td_record.length).to eq 1
        prov_add_xattr(td_record[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
        prov_add_xattr(td_record[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check training dataset" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
        expect(result1.length).to eq 1
        xattrs = Hash.new
        xattrs["xattr_key_1"] = "xattr_value_1"
        xattrs["xattr_key_2"] = "xattr_value_2"
        prov_check_asset_with_xattrs(result1, prov_td_id(@td1_name, @td_version1), xattrs)
      end

      it "delete training dataset" do
        prov_delete_td(@project1, @td1_name, @td_version1)
      end

      it "check training dataset" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
        expect(result1.length).to eq 0
      end
    end
    describe "training dataset with simple xattr count" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create training dataset with xattr" do
        prov_create_td(@project1, @td1_name, @td_version1)
        td_record1 = prov_get_td_record(@project1, @td1_name, @td_version1)
        expect(td_record1.length).to eq 1
        prov_add_xattr(td_record1[0], "key", "val1", "XATTR_ADD", 1)

        prov_create_td(@project1, @td1_name, @td_version2)
        td_record2 = prov_get_td_record(@project1, @td1_name, @td_version2)
        expect(td_record2.length).to eq 1
        prov_add_xattr(td_record2[0], "key", "val1", "XATTR_ADD", 1)

        prov_create_td(@project1, @td2_name, @td_version1)
        td_record3 = prov_get_td_record(@project1, @td2_name, @td_version1)
        expect(td_record3.length).to eq 1
        prov_add_xattr(td_record3[0], "key", "val2", "XATTR_ADD", 1)

        prov_create_td(@project2, @td2_name, @td_version1)
        td_record4 = prov_get_td_record(@project2, @td2_name, @td_version1)
        expect(td_record4.length).to eq 1
        prov_add_xattr(td_record4[0], "key", "val2", "XATTR_ADD", 1)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check training dataset" do 
        prov_wait_for_epipe() 
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "key", "val1", 2)
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "key", "val2", 1)
        get_ml_asset_by_xattr_count(@project2, "TRAINING_DATASET", "key", "val2", 1)
      end

      it "delete training dataset" do
        prov_delete_td(@project1, @td1_name, @td_version1)
        prov_delete_td(@project1, @td1_name, @td_version2)
        prov_delete_td(@project1, @td2_name, @td_version1)
        prov_delete_td(@project2, @td2_name, @td_version1)
      end

      it "check training dataset" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
        expect(result1.length).to eq 0
      end
    end

    describe "training dataset with nested xattr count" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create training dataset with features count" do
        prov_create_td(@project1, @td1_name, @td_version1)
        td_record1 = prov_get_td_record(@project1, @td1_name, @td_version1)
        expect(td_record1.length).to eq 1
        xattr1Json = JSON['{"group":"group","value":"val1","version":"version"}']
        prov_add_xattr(td_record1[0], "features", JSON[xattr1Json], "XATTR_ADD", 1)

        prov_create_td(@project1, @td1_name, @td_version2)
        td_record2 = prov_get_td_record(@project1, @td1_name, @td_version2)
        expect(td_record2.length).to eq 1
        prov_add_xattr(td_record2[0], "features", JSON[xattr1Json], "XATTR_ADD", 1)

        prov_create_td(@project1, @td2_name, @td_version1)
        td_record3 = prov_get_td_record(@project1, @td2_name, @td_version1)
        expect(td_record3.length).to eq 1
        xattr2Json = JSON['{"group":"group","value":"val2","version":"version"}']
        prov_add_xattr(td_record3[0], "features", JSON[xattr2Json], "XATTR_ADD", 1)

        prov_create_td(@project2, @td2_name, @td_version1)
        td_record4 = prov_get_td_record(@project2, @td2_name, @td_version1)
        expect(td_record4.length).to eq 1
        prov_add_xattr(td_record4[0], "features", JSON[xattr2Json], "XATTR_ADD", 1)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check training dataset" do 
        prov_wait_for_epipe() 
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "features.value", "val1", 2)
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "features.value", "val2", 1)
        get_ml_asset_by_xattr_count(@project2, "TRAINING_DATASET", "features.value", "val2", 1)
      end

      it "delete training dataset" do
        prov_delete_td(@project1, @td1_name, @td_version1)
        prov_delete_td(@project1, @td1_name, @td_version2)
        prov_delete_td(@project1, @td2_name, @td_version1)
        prov_delete_td(@project2, @td2_name, @td_version1)
      end

      it "check training dataset" do 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
        expect(result1.length).to eq 0
      end
    end
  end 

  describe "ml asset (experiment) xattr tests" do
    def check_xattr(xattr, expected_xattr)
      #pp xattr
      xattr1 = xattr.slice(xattr.index("value=")..-1)
      xattr2 = "{#{xattr1}"
      xattr3 = xattr2.gsub('=', ':')
      xattr4 = xattr3.gsub(/([a-z_1-9]+):/, '"\1":')
      xattr5 = xattr4.gsub(/:([a-z_1-9]+)/, ':"\1"')
      xattr6 = JSON.parse(xattr5)
      #pp xattr6["value"]
      #pp expected_xattr
      expect(xattr6["value"]).to eq expected_xattr
    end

    describe "add xattr" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV1], "XATTR_ADD", 1)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id_2(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_xattr(test_xattr_field[0]["value"], @xattrV1)
      end

      it "delete experiment dataset" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false, 404)
      end
    end
    describe "update xattr" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV1], "XATTR_ADD", 1)
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV2], "XATTR_UPDATE", 2)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id_2(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_xattr(test_xattr_field[0]["value"], @xattrV2)
      end

      it "delete experiment dataset" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false, 404)
      end
    end
    describe "delete xattr" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment1_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment1_record.length).to eq 1
        prov_add_xattr(experiment1_record[0], "test_xattr", JSON[@xattrV1], "XATTR_ADD", 1)
        prov_add_xattr(experiment1_record[0], "test_xattr", "", "XATTR_DELETE", 2)

        prov_create_experiment(@project1, @experiment_app2_name1)
        experiment2_record = prov_get_experiment_record(@project1, @experiment_app2_name1)
        expect(experiment2_record.length).to eq 1
        prov_add_xattr(experiment2_record[0], "test_xattr", JSON[@xattrV1], "XATTR_ADD", 1)
        prov_add_xattr(experiment2_record[0], "test_xattr", JSON[@xattrV2], "XATTR_UPDATE", 2)
        prov_add_xattr(experiment2_record[0], "test_xattr", "", "XATTR_DELETE", 3)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe()
        experiment1_id = prov_experiment_id(@experiment_app1_name1)
        experiment1 = get_ml_asset_by_id_2(@project1, "EXPERIMENT", experiment1_id, false)
        #pp experiment
        experiment1_xattr_field = experiment1["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(experiment1_xattr_field.length).to eq 0

        experiment2_id = prov_experiment_id(@experiment_app2_name1)
        experiment2 = get_ml_asset_by_id_2(@project1, "EXPERIMENT", experiment2_id, false)
        #pp experiment
        experiment2_xattr_field = experiment2["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(experiment1_xattr_field.length).to eq 0
      end

      it "delete experiment dataset" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app2_name1)
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe() 
        experiment1_id = prov_experiment_id(@experiment_app1_name1)
        get_ml_asset_by_id(@project1, "EXPERIMENT", experiment1_id, false, 404)
        experiment2_id = prov_experiment_id(@experiment_app2_name1)
        get_ml_asset_by_id(@project1, "EXPERIMENT", experiment2_id, false, 404)
      end
    end

    def check_not_json_xattr(xattr, expected_xattr)
      #pp xattr
      xattr1 = xattr.gsub('=', ':')
      xattr2 = xattr1.gsub(/([a-zA-Z_1-9]+):/, '"\1":')
      xattr3 = xattr2.gsub(/:([a-zA-Z_1-9]+)/, ':"\1"')
      xattr4 = JSON.parse(xattr3)
      #pp xattr4["raw"]
      #pp expected_xattr
      expect(xattr4["raw"]).to eq expected_xattr
    end

    def check_xattr(xattr, expected_xattr)
      #pp xattr
      xattr1 = xattr.slice(xattr.index("value=")..-1)
      xattr2 = "{#{xattr1}"
      xattr3 = xattr2.gsub('=', ':')
      xattr4 = xattr3.gsub(/([a-zA-Z_1-9]+):/, '"\1":')
      xattr5 = xattr4.gsub(/:([a-zA-Z_1-9]+)/, ':"\1"')
      xattr6 = JSON.parse(xattr5)
      #pp xattr6["value"]
      #pp expected_xattr
      expect(xattr6["value"]).to eq expected_xattr
    end

    describe "array xattr" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV3], "XATTR_ADD", 1)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id_2(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_xattr(test_xattr_field[0]["value"], @xattrV3)
      end

      it "delete experiment dataset" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false, 404)
      end
    end

    describe "not json xattr" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr", @xattrV4, "XATTR_ADD", 1)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id_2(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_not_json_xattr(test_xattr_field[0]["value"], @xattrV4)
      end

      it "delete experiment dataset" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
      end

      it "check experiment dataset" do 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false, 404)
      end
    end

    describe "search by xattr" do
      it "stop epipe" do
        execute_remotely @hostname, "sudo systemctl stop epipe"
      end

      it "create experiment with xattr" do
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr1", JSON[@xattrV1], "XATTR_ADD", 1)
        prov_add_xattr(experiment_record[0], "test_xattr2", JSON[@xattrV3], "XATTR_ADD", 2)
        prov_add_xattr(experiment_record[0], "test_xattr4", @xattrV4, "XATTR_ADD", 3)
        prov_add_xattr(experiment_record[0], "test_xattr5", JSON[@xattrV5], "XATTR_ADD", 4)
      end

      it "restart epipe" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end

      it "check simple json - ok - search result" do 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)

        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr1.f1_1", "v1")
        #pp experiment
        expect(experiment.length).to eq 1
        prov_check_asset_with_id(experiment, experiment_id)

        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr1.f1_2.f2_1", "val1")
        #pp experiment
        expect(experiment.length).to eq 1
        prov_check_asset_with_id(experiment, experiment_id)
      end

      it "check simple json - not ok - search result" do 
        experiment_id = prov_experiment_id(@experiment_app1_name1)

        #bad key
        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr1.f1_1_1", "v1")
        #pp experiment
        expect(experiment.length).to eq 0

        #bad value
        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr1.f1_1", "val12")
        #pp experiment
        expect(experiment.length).to eq 0
      end

      it "check json array - ok - search result" do 
        experiment_id = prov_experiment_id(@experiment_app1_name1)

        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr2.f3_1", "val1")
        #pp experiment
        expect(experiment.length).to eq 1
        prov_check_asset_with_id(experiment, experiment_id)

        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr5.f3_1", "val1")
        #pp experiment
        expect(experiment.length).to eq 1
        prov_check_asset_with_id(experiment, experiment_id)

        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr5.f3_1", "val2")
        #pp experiment
        expect(experiment.length).to eq 1
        prov_check_asset_with_id(experiment, experiment_id)
      end

      it "check not json - ok - search result" do 
        experiment_id = prov_experiment_id(@experiment_app1_name1)

        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr4", "notJson")
        #pp experiment
        expect(experiment.length).to eq 1
        prov_check_asset_with_id(experiment, experiment_id)
      end

      it "check not json - not ok - search result" do 
        experiment_id = prov_experiment_id(@experiment_app1_name1)

        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr4", "notJson1")
        #pp experiment
        expect(experiment.length).to eq 0
      end

      it "delete experiment dataset" do
        prov_delete_experiment(@project1, @experiment_app1_name1)
      end

      it "check cleanup" do 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false, 404)
      end
    end 
  end

  describe 'mock app footprint' do
    it "stop epipe" do
      execute_remotely @hostname, "sudo systemctl stop epipe"
    end

    it "create mock footprint" do
      prov_create_experiment(@project1, @experiment_app1_name1)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name1)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app1_id}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 200000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["io_app_id"] = @app1_id
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = 1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_2_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_2_op_1["inode_id"] = 200001
      experiment_mock_file_2_op_1["io_timestamp"] = experiment_mock_file_2_op_1["io_timestamp"]+1
      experiment_mock_file_2_op_1["i_name"] = "mock_file_2"
      experiment_mock_file_2_op_1.save!

      experiment_mock_file_3_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_3_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_3_op_1["inode_id"] = 200002
      experiment_mock_file_3_op_1["io_timestamp"] = experiment_mock_file_3_op_1["io_timestamp"]+1
      experiment_mock_file_3_op_1["io_app_id"] = @app2_id
      experiment_mock_file_3_op_1["i_name"] = "mock_file_3"
      experiment_mock_file_3_op_1.save!
    end

    it "restart epipe" do
      execute_remotely @hostname, "sudo systemctl restart epipe"
    end

    it "check mock footprint" do 
      prov_wait_for_epipe() 
      result = get_app_footprint(@project1, @app1_id, "FULL")
      # pp result
      expect(result.length).to eq 3

      result = get_app_footprint(@project1, @app1_id, "COMPACT")
      # pp result
      expect(result.length).to eq 2
    end
  end
end