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
    @experiment_app1_name3 = "#{@app1_id}_3"
    @app_file_history1   = "application_#{short_random_id}_fh1"
    @app_file_history2   = "application_#{short_random_id}_fh2"
    @app_file_history3   = "application_#{short_random_id}_fh3"
    @app_file_ops1      = "application_#{short_random_id}_fo1"
    @app_file_ops2      = "application_#{short_random_id}_fo2"
    @app_app_fprint     = "application_#{short_random_id}_af"
    @experiment_file_history    = "#{@app_file_history1}_1"
    @experiment_file_ops        = "#{@app_file_ops1}_1"
    @experiment_app_fprint      = "#{@app_app_fprint}_1"
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
    @xattrV6 = "notJava"
    @xattrV7 = "not Json"
    @xattrV8 = JSON['{"name": "fashion mnist gridsearch"}']
    # pp "create project: #{@project1_name}"
    # @project1 = create_project_by_name(@project1_name)
    # pp "create project: #{@project2_name}"
    # @project2 = create_project_by_name(@project2_name)
  end
  
  it "test epipe" do
    pp "restart epipe"
    execute_remotely @hostname, "sudo systemctl restart epipe"

    pp "wait for epipe"
    prov_wait_for_epipe() 
  end

  def cleanup(project, ops)
    ops["filesInProject"]["items"].each do |file|
      aux = file_ops_archival(project, file["inodeId"])
      #pp "#{aux} #{file["count"]}"
      expect(aux["cleaned"]).to eq file["count"]
    end
  end

  describe 'test suite - 2 projects' do
    before :all do
      pp "create project: #{@project1_name}"
      @project1 = create_project_by_name(@project1_name)
      pp "create project: #{@project2_name}"
      @project2 = create_project_by_name(@project2_name)
    end

    after :each do 
      pp "cleanup provenance index" 
      sleep(1)
      ops = get_file_ops_archival(@project1)
      #pp ops
      cleanup(@project1, ops)
      ops = get_file_ops_archival(@project2)
      #pp ops
      cleanup(@project2, ops)

      pp "check provenance index cleanup" 
      sleep(1)
      ops = get_file_ops_archival(@project1)
      #pp ops
      expect(ops["count"]).to eq 0
      ops = get_file_ops_archival(@project2)
      #pp ops
      expect(ops["count"]).to eq 0
    end

    after :all do 
      pp "delete projects"
      delete_project(@project1)
      @project1 = nil
      delete_project(@project2)
      @project2 = nil
    end

    describe 'provenance tests - experiments' do
      it 'simple experiments'  do
        pp "check epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "create experiments"
        prov_create_experiment(@project1, @experiment_app1_name1)
        prov_create_experiment(@project1, @experiment_app2_name1)
        prov_create_experiment(@project2, @experiment_app3_name1)

        pp "check experiments" 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false, 2)["items"]
        prov_check_asset_with_id(result1, prov_experiment_id(@experiment_app1_name1))
        prov_check_asset_with_id(result1, prov_experiment_id(@experiment_app2_name1))

        result2 = get_ml_asset_in_project(@project2, "EXPERIMENT", false, 1)["items"]
        prov_check_asset_with_id(result2, prov_experiment_id(@experiment_app3_name1))
        
        result3 = get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app2_name1)
        prov_delete_experiment(@project2, @experiment_app3_name1)
        
        pp "check hops cleanup"  
        prov_wait_for_epipe() 
        get_ml_asset_in_project(@project1, "EXPERIMENT", false, 0)
        get_ml_asset_in_project(@project2, "EXPERIMENT", false, 0)     
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
      end
    end
    describe "models" do
      it 'simple models' do
        pp "create models"
        prov_create_model(@project1, @model1_name)
        prov_create_model_version(@project1, @model1_name, @model_version1)
        prov_create_model_version(@project1, @model1_name, @model_version2)
        prov_create_model(@project1, @model2_name)
        prov_create_model_version(@project1, @model2_name, @model_version1)
        prov_create_model(@project2, @model1_name)
        prov_create_model_version(@project2, @model1_name, @model_version1)

        pp "check models"
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL", false, 3)["items"]
        prov_check_asset_with_id(result1, prov_model_id(@model1_name, @model_version1))
        prov_check_asset_with_id(result1, prov_model_id(@model1_name, @model_version2))
        prov_check_asset_with_id(result1, prov_model_id(@model2_name, @model_version1))

        result2 = get_ml_asset_in_project(@project2, "MODEL", false, 1)["items"]
        prov_check_asset_with_id(result2, prov_model_id(@model1_name, @model_version1))
        
        result3 = get_ml_asset_by_id(@project1, "MODEL", prov_model_id(@model1_name, @model_version2), false)

        pp "cleanup hops"
        prov_delete_model(@project1, @model1_name)
        prov_delete_model(@project1, @model2_name)
        prov_delete_model(@project2, @model1_name)
        
        pp "check hopscleanup" 
        prov_wait_for_epipe() 
        get_ml_asset_in_project(@project1, "MODEL", false, 0)
        get_ml_asset_in_project(@project2, "MODEL", false, 0)
        result3 = check_no_ml_asset_by_id(@project1, "MODEL", prov_model_id(@model1_name, @model_version2), false)
      end
    end
    describe 'training datasets' do
      it 'simple training datasets' do
        pp "create training datasets"
        prov_create_td(@project1, @td1_name, @td_version1)
        prov_create_td(@project1, @td1_name, @td_version2)
        prov_create_td(@project1, @td2_name, @td_version1)
        prov_create_td(@project2, @td1_name, @td_version1)

        pp "check training datasets" 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false, 3)["items"]
        prov_check_asset_with_id(result1, prov_td_id(@td1_name, @td_version1))
        prov_check_asset_with_id(result1, prov_td_id(@td1_name, @td_version2))
        prov_check_asset_with_id(result1, prov_td_id(@td2_name, @td_version1))

        result2 = get_ml_asset_in_project(@project2, "TRAINING_DATASET", false, 1)["items"]
        prov_check_asset_with_id(result2, prov_td_id(@td1_name, @td_version1))
        
        result3 = get_ml_asset_by_id(@project1, "TRAINING_DATASET", prov_td_id(@td1_name, @td_version1), false)

        pp "cleanup hops"
        prov_delete_td(@project1, @td1_name, @td_version1)
        prov_delete_td(@project1, @td1_name, @td_version2)
        prov_delete_td(@project1, @td2_name, @td_version1)
        prov_delete_td(@project2, @td1_name, @td_version1)
        
        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        get_ml_asset_in_project(@project1, "TRAINING_DATASET", false, 0)
        get_ml_asset_in_project(@project2, "TRAINING_DATASET", false, 0)
        result3 = check_no_ml_asset_by_id(@project1, "TRAINING_DATASET", prov_td_id(@td1_name, @td_version1), false)
      end

      it "training dataset with simple xattr count"  do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create training dataset with xattr"
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

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check training dataset" 
        prov_wait_for_epipe() 
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "key", "val1", 2)
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "key", "val2", 1)
        get_ml_asset_by_xattr_count(@project2, "TRAINING_DATASET", "key", "val2", 1)

        pp "cleanup hops"
        prov_delete_td(@project1, @td1_name, @td_version1)
        prov_delete_td(@project1, @td1_name, @td_version2)
        prov_delete_td(@project1, @td2_name, @td_version1)
        prov_delete_td(@project2, @td2_name, @td_version1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        get_ml_asset_in_project(@project1, "TRAINING_DATASET", false, 0)
      end

      it "training dataset with nested xattr count" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create training dataset with features count"
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

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check training dataset" 
        prov_wait_for_epipe() 
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "features.value", "val1", 2)
        get_ml_asset_by_xattr_count(@project1, "TRAINING_DATASET", "features.value", "val2", 1)
        get_ml_asset_by_xattr_count(@project2, "TRAINING_DATASET", "features.value", "val2", 1)

        pp "cleanup hops"
        prov_delete_td(@project1, @td1_name, @td_version1)
        prov_delete_td(@project1, @td1_name, @td_version2)
        prov_delete_td(@project1, @td2_name, @td_version1)
        prov_delete_td(@project2, @td2_name, @td_version1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        get_ml_asset_in_project(@project1, "TRAINING_DATASET", false, 0)
      end
    end
  end

  describe 'test suite - 1 project' do
    before :all do
      pp "create project: #{@project1_name}"
      @project1 = create_project_by_name(@project1_name)
    end

    after :each do
      pp "cleanup provenance index"
      sleep(1)
      ops = get_file_ops_archival(@project1)
      #pp ops
      cleanup(@project1, ops)

      pp "check provenance index cleanup"
      sleep(1)
      ops = get_file_ops_archival(@project1)
      #pp ops
      expect(ops["count"]).to eq 0
    end

    after :all do 
      pp "delete projects"
      delete_project(@project1)
      @project1 = nil
    end

    it 'experiment with xattr' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create experiment with xattr"
      prov_create_experiment(@project1, @experiment_app1_name1)
      experimentRecord = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name1)
      expect(experimentRecord.length).to eq 1
      prov_add_xattr(experimentRecord[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
      prov_add_xattr(experimentRecord[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"

      pp "check experiment" 
      prov_wait_for_epipe() 
      result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false, 1)["items"]
      xattrsExact = Hash.new
      xattrsExact["xattr_key_1"] = "xattr_value_1"
      xattrsExact["xattr_key_2"] = "xattr_value_2"
      prov_check_asset_with_xattrs(result1, prov_experiment_id(@experiment_app1_name1), xattrsExact)

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_app1_name1)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      get_ml_asset_in_project(@project1, "EXPERIMENT", false, 0)
    end

    it 'experiment with xattr add, update and delete' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create experiment with xattr"
      prov_create_experiment(@project1, @experiment_app1_name1)
      experimentRecord = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app1_name1)
      expect(experimentRecord.length).to eq 1
      prov_add_xattr(experimentRecord[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
      prov_add_xattr(experimentRecord[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)
      prov_add_xattr(experimentRecord[0], "xattr_key_3", "xattr_value_3", "XATTR_ADD", 3)
      prov_add_xattr(experimentRecord[0], "xattr_key_1", "xattr_value_1_updated", "XATTR_UPDATE", 4)
      prov_add_xattr(experimentRecord[0], "xattr_key_2", "", "XATTR_DELETE", 5)

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"

      pp "check experiment" 
      prov_wait_for_epipe() 
      result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", false, 1)["items"]
      xattrsExact = Hash.new
      xattrsExact["xattr_key_1"] = "xattr_value_1_updated"
      xattrsExact["xattr_key_3"] = "xattr_value_3"
      prov_check_asset_with_xattrs(result1, prov_experiment_id(@experiment_app1_name1), xattrsExact)

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_app1_name1)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      get_ml_asset_in_project(@project1, "EXPERIMENT", false, 0)
    end

    it 'experiment with app states' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create experiment with app states"
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

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"

      pp "check experiment" 
      prov_wait_for_epipe() 
      result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", true, 3)["items"]
      prov_check_experiment3(result1, prov_experiment_id(@experiment_app1_name1), "RUNNING")
      prov_check_experiment3(result1, prov_experiment_id(@experiment_app1_name2), "RUNNING")
      prov_check_experiment3(result1, prov_experiment_id(@experiment_app2_name1), "FINISHED")

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_app1_name1)
      prov_delete_experiment(@project1, @experiment_app1_name2)
      prov_delete_experiment(@project1, @experiment_app2_name1)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      get_ml_asset_in_project(@project1, "EXPERIMENT", true, 0)
    end

    it 'not experiment in Experiments' do
      pp "create not experiment dir"
      prov_create_experiment(@project1, @not_experiment_name)
      
      pp "check not experiment" 
      prov_wait_for_epipe() 
      get_ml_asset_in_project(@project1, "EXPERIMENT", false, 0)

      pp "delete not experiment"
      prov_delete_experiment(@project1, @not_experiment_name)
    end

    describe "models" do
      it 'model with xattr' do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create model with xattr"
        prov_create_model(@project1, @model1_name)
        prov_create_model_version(@project1, @model1_name, @model_version1)
        modelRecord = FileProv.where("project_name": @project1["inode_name"], "i_parent_name": @model1_name, "i_name": @model_version1)
        expect(modelRecord.length).to eq 1
        prov_add_xattr(modelRecord[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
        prov_add_xattr(modelRecord[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check model" 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL", false, 1)["items"]
        xattrsExact = Hash.new
        xattrsExact["xattr_key_1"] = "xattr_value_1"
        xattrsExact["xattr_key_2"] = "xattr_value_2"
        prov_check_asset_with_xattrs(result1, prov_model_id(@model1_name, @model_version1), xattrsExact)

        pp "cleanup hops"
        prov_delete_model(@project1, @model1_name)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        get_ml_asset_in_project(@project1, "MODEL", false, 0)
      end
    end

    describe 'training datasets' do
      it 'training dataset with xattr' do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create training dataset with xattr"
        prov_create_td(@project1, @td1_name, @td_version1)
        td_record = prov_get_td_record(@project1, @td1_name, @td_version1)
        expect(td_record.length).to eq 1
        prov_add_xattr(td_record[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
        prov_add_xattr(td_record[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check training dataset" 
        prov_wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false, 1)["items"]
        xattrsExact = Hash.new
        xattrsExact["xattr_key_1"] = "xattr_value_1"
        xattrsExact["xattr_key_2"] = "xattr_value_2"
        prov_check_asset_with_xattrs(result1, prov_td_id(@td1_name, @td_version1), xattrsExact)

        pp "cleanup hops"
        prov_delete_td(@project1, @td1_name, @td_version1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        get_ml_asset_in_project(@project1, "TRAINING_DATASET", false, 0)
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

      it "add xattr" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV1], "XATTR_ADD", 1)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check experiment dataset" 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_xattr(test_xattr_field[0]["value"], @xattrV1)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
      end

      it "update xattr" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV1], "XATTR_ADD", 1)
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV2], "XATTR_UPDATE", 2)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check experiment dataset" 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_xattr(test_xattr_field[0]["value"], @xattrV2)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
      end
      it "delete xattr" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
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

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check experiment dataset" 
        prov_wait_for_epipe()
        experiment1_id = prov_experiment_id(@experiment_app1_name1)
        experiment1 = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment1_id, false)
        #pp experiment
        experiment1_xattr_field = experiment1["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(experiment1_xattr_field.length).to eq 0

        experiment2_id = prov_experiment_id(@experiment_app2_name1)
        experiment2 = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment2_id, false)
        #pp experiment
        experiment2_xattr_field = experiment2["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(experiment1_xattr_field.length).to eq 0

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app2_name1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment1_id = prov_experiment_id(@experiment_app1_name1)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment1_id, false)
        experiment2_id = prov_experiment_id(@experiment_app2_name1)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment2_id, false)
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

      it "array xattr" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr", JSON[@xattrV3], "XATTR_ADD", 1)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check experiment dataset" 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_xattr(test_xattr_field[0]["value"], @xattrV3)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
      end

      it "not json xattr" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr", @xattrV4, "XATTR_ADD", 1)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check experiment dataset" 
        prov_wait_for_epipe()
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
        #pp experiment
        test_xattr_field = experiment["map"]["entry"].select { |e| e["key"] == "test_xattr"}
        expect(test_xattr_field.length).to eq 1
        check_not_json_xattr(test_xattr_field[0]["value"], @xattrV4)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
      end

      it "search by xattr" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record.length).to eq 1  
        prov_add_xattr(experiment_record[0], "test_xattr1", JSON[@xattrV1], "XATTR_ADD", 1)
        prov_add_xattr(experiment_record[0], "test_xattr2", JSON[@xattrV3], "XATTR_ADD", 2)
        prov_add_xattr(experiment_record[0], "test_xattr4", @xattrV4, "XATTR_ADD", 3)
        prov_add_xattr(experiment_record[0], "test_xattr5", JSON[@xattrV5], "XATTR_ADD", 4)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check simple json - ok - search result" 
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

        pp "check simple json - not ok - search result" 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        #bad key
        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr1.f1_1_1", "v1")
        #pp experiment
        expect(experiment.length).to eq 0
        #bad value
        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr1.f1_1", "val12")
        #pp experiment
        expect(experiment.length).to eq 0

        pp "check json array - ok - search result" 
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

        pp "check not json - ok - search result" 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr4", "notJson")
        #pp experiment
        expect(experiment.length).to eq 1
        prov_check_asset_with_id(experiment, experiment_id)

        pp "check not json - not ok - search result" 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        experiment = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr4", "notJson1")
        #pp experiment
        expect(experiment.length).to eq 0

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment_id = prov_experiment_id(@experiment_app1_name1)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id, false)
      end 

      it "search by like xattr" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record1 = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record1.length).to eq 1  
        prov_add_xattr(experiment_record1[0], "test_xattr", @xattrV4, "XATTR_ADD", 1)
        prov_create_experiment(@project1, @experiment_app2_name1)
        experiment_record2 = prov_get_experiment_record(@project1, @experiment_app2_name1)
        expect(experiment_record2.length).to eq 1  
        prov_add_xattr(experiment_record2[0], "test_xattr", @xattrV6, "XATTR_ADD", 1)
        prov_create_experiment(@project1, @experiment_app1_name2)
        experiment_record3 = prov_get_experiment_record(@project1, @experiment_app1_name2)
        expect(experiment_record3.length).to eq 1  
        prov_add_xattr(experiment_record3[0], "test_xattr", @xattrV7, "XATTR_ADD", 1)
        prov_add_xattr(experiment_record3[0], "config", JSON[@xattrV8], "XATTR_ADD", 2)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"
        prov_wait_for_epipe() 

        pp "check not json - ok - search result" 
        experiment1_id = prov_experiment_id(@experiment_app1_name1)
        experiment2_id = prov_experiment_id(@experiment_app2_name1)
        experiment3_id = prov_experiment_id(@experiment_app1_name2)
        experiments1 = get_ml_asset_by_xattr(@project1, "EXPERIMENT", "test_xattr", "notJson")
        #pp experiment
        expect(experiments1.length).to eq 1
        prov_check_asset_with_id(experiments1, experiment1_id)
        experiments2 = get_ml_asset_like_xattr(@project1, "EXPERIMENT", "test_xattr", "notJson")
        #pp experiment
        expect(experiments2.length).to eq 1
        prov_check_asset_with_id(experiments2, experiment1_id)
        experiments3 = get_ml_asset_like_xattr(@project1, "EXPERIMENT", "test_xattr", "notJs")
        #pp experiment
        expect(experiments3.length).to eq 1
        prov_check_asset_with_id(experiments3, experiment1_id)      
        experiments4 = get_ml_asset_like_xattr(@project1, "EXPERIMENT", "test_xattr", "not")
        #pp experiment
        expect(experiments4.length).to eq 3
        prov_check_asset_with_id(experiments4, experiment1_id)
        prov_check_asset_with_id(experiments4, experiment2_id)
        prov_check_asset_with_id(experiments4, experiment3_id)
        experiments5 = get_ml_asset_like_xattr(@project1, "EXPERIMENT", "test_xattr", "Json")
        #pp experiment
        expect(experiments5.length).to eq 2
        prov_check_asset_with_id(experiments5, experiment1_id)
        prov_check_asset_with_id(experiments5, experiment3_id)
        experiments6 = get_ml_asset_like_xattr(@project1, "EXPERIMENT", "config.name", "mnist")
        #pp experiment
        expect(experiments6.length).to eq 1
        prov_check_asset_with_id(experiments6, experiment3_id)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app2_name1)
        prov_delete_experiment(@project1, @experiment_app1_name2)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment_id1 = prov_experiment_id(@experiment_app1_name1)
        experiment_id2 = prov_experiment_id(@experiment_app2_name1)
        experiment_id3 = prov_experiment_id(@experiment_app1_name2)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id2, false)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id3, false)
      end 

      it "search by like xattr 2" do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        prov_create_experiment(@project1, @experiment_app1_name1)
        experiment_record1 = prov_get_experiment_record(@project1, @experiment_app1_name1)
        expect(experiment_record1.length).to eq 1  
        prov_add_xattr(experiment_record1[0], "config", JSON[@xattrV8], "XATTR_ADD", 1)
        prov_create_experiment(@project1, @experiment_app1_name2)
        experiment_record2 = prov_get_experiment_record(@project1, @experiment_app1_name2)
        expect(experiment_record2.length).to eq 1  
        prov_add_xattr(experiment_record2[0], "config", JSON[@xattrV8], "XATTR_ADD", 1)
        prov_create_experiment(@project1, @experiment_app1_name3)
        experiment_record3 = prov_get_experiment_record(@project1, @experiment_app1_name3)
        expect(experiment_record3.length).to eq 1  
        prov_add_xattr(experiment_record3[0], "config", JSON[@xattrV8], "XATTR_ADD", 1)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"
        prov_wait_for_epipe() 

        pp "check not json - ok - search result" 
        experiment1_id = prov_experiment_id(@experiment_app1_name1)
        experiment2_id = prov_experiment_id(@experiment_app1_name2)
        experiment3_id = prov_experiment_id(@experiment_app1_name3)
        experiments1 = get_ml_asset_like_xattr(@project1, "EXPERIMENT", "config.name", "mnist")
        #pp experiment
        expect(experiments1.length).to eq 3
        prov_check_asset_with_id(experiments1, experiment1_id)
        prov_check_asset_with_id(experiments1, experiment2_id)
        prov_check_asset_with_id(experiments1, experiment3_id)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app1_name2)
        prov_delete_experiment(@project1, @experiment_app1_name3)

        pp "check hops cleanup" 
        prov_wait_for_epipe() 
        experiment_id1 = prov_experiment_id(@experiment_app1_name1)
        experiment_id2 = prov_experiment_id(@experiment_app1_name2)
        experiment_id3 = prov_experiment_id(@experiment_app1_name3)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id2, false)
        check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id3, false)
      end
    end

    it 'mock app file operations' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create mock file operations - file operations"
      prov_create_experiment(@project1, @experiment_file_ops)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_file_ops)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_file_ops1}", "XATTR_ADD", 1)
      #pp experiment_record
      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 100000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_file_ops1
      experiment_mock_file_1_op_1.save!
      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = 1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!
      experiment_mock_file_2_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_2_op_1["inode_id"] = 100001
      experiment_mock_file_2_op_1["io_timestamp"] = experiment_mock_file_2_op_1["io_timestamp"]+1
      experiment_mock_file_2_op_1["i_name"] = "mock_file_2"
      experiment_mock_file_2_op_1.save!
      experiment_mock_file_3_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_3_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_3_op_1["inode_id"] = 100002
      experiment_mock_file_3_op_1["io_timestamp"] = experiment_mock_file_3_op_1["io_timestamp"]+1
      experiment_mock_file_3_op_1["io_app_id"] = @app_file_ops2
      experiment_mock_file_3_op_1["i_name"] = "mock_file_3"
      experiment_mock_file_3_op_1.save!

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 

      pp "check mock fileOperations" 
      result = get_app_file_ops(@project1, @app_file_ops1, "NONE", "LIST")
      #pp result
      expect(result["items"].length).to eq 3
      expect(result["count"]).to eq 3
      result = get_app_file_ops(@project1, @app_file_ops1, "FILE_COMPACT", "LIST")
      # pp result
      expect(result.length).to eq 2

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_file_ops)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_file_ops)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
    end

    it 'mock app footprint' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create mock file operations - app footprint"
      prov_create_experiment(@project1, @experiment_app_fprint)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app_fprint)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_app_fprint}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 200000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_out_added_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_app_fprint
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = experiment_mock_file_1_op_2["io_logical_time"]+1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_2_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_2_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_2_op_1["inode_id"] = 200001
      experiment_mock_file_2_op_1["io_logical_time"] = 0
      experiment_mock_file_2_op_1["io_timestamp"] = experiment_mock_file_2_op_1["io_timestamp"]+1
      experiment_mock_file_2_op_1["i_name"] = "mock_file_out_added_2"
      experiment_mock_file_2_op_1.save!

      experiment_mock_file_3_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_3_op_1["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_3_op_1["inode_id"] = 200002
      experiment_mock_file_3_op_1["io_logical_time"] = 1
      experiment_mock_file_3_op_1["io_timestamp"] = experiment_mock_file_3_op_1["io_timestamp"]+1
      experiment_mock_file_3_op_1["i_name"] = "mock_file_out_1"
      experiment_mock_file_3_op_1.save!

      experiment_mock_file_4_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_4_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_4_op_1["inode_id"] = 200003
      experiment_mock_file_4_op_1["io_logical_time"] = 0
      experiment_mock_file_4_op_1["io_timestamp"] = experiment_mock_file_4_op_1["io_timestamp"]+1
      experiment_mock_file_4_op_1["i_name"] = "mock_file_tmp_1"
      experiment_mock_file_4_op_1.save!

      experiment_mock_file_4_op_2 = experiment_mock_file_4_op_1.dup
      experiment_mock_file_4_op_2["inode_operation"] = "DELETE"
      experiment_mock_file_4_op_2["io_logical_time"] = experiment_mock_file_4_op_2["io_logical_time"]+1
      experiment_mock_file_4_op_2["io_timestamp"] = experiment_mock_file_4_op_2["io_timestamp"]+1
      experiment_mock_file_4_op_2.save!

      experiment_mock_file_5_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_5_op_1["inode_operation"] = "DELETE"
      experiment_mock_file_5_op_1["inode_id"] = 200004
      experiment_mock_file_5_op_1["io_logical_time"] = 1
      experiment_mock_file_5_op_1["io_timestamp"] = experiment_mock_file_5_op_1["io_timestamp"]+1
      experiment_mock_file_5_op_1["i_name"] = "mock_file_rm_1"
      experiment_mock_file_5_op_1.save!

      experiment_mock_file_6_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_6_op_1["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_6_op_1["inode_id"] = 200005
      experiment_mock_file_6_op_1["io_logical_time"] = 1
      experiment_mock_file_6_op_1["io_timestamp"] = experiment_mock_file_6_op_1["io_timestamp"]+1
      experiment_mock_file_6_op_1["i_name"] = "mock_file_in_1"
      experiment_mock_file_6_op_1.save!

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 

      pp "check mock app footprint" 
      result = get_app_footprint(@project1, @app_app_fprint, "ALL")
      # pp result
      expect(result.length).to eq 6

      result = get_app_footprint(@project1, @app_app_fprint, "INPUT")
      # pp result
      expect(result.length).to eq 1

      result = get_app_footprint(@project1, @app_app_fprint, "OUTPUT")
      # pp result
      expect(result.length).to eq 3

      result = get_app_footprint(@project1, @app_app_fprint, "OUTPUT_ADDED")
      # pp result
      expect(result.length).to eq 2

      result = get_app_footprint(@project1, @app_app_fprint, "TMP")
      # pp result
      expect(result.length).to eq 1

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_app_fprint)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_app_fprint)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
    end

    it 'mock file history' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create mock file history"
      prov_create_experiment(@project1, @experiment_file_history)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_file_history)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_file_history1}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 300000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_file_history1
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = experiment_mock_file_1_op_2["io_logical_time"]+1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_1_op_3 = experiment_mock_file_1_op_2.dup
      experiment_mock_file_1_op_3["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_1_op_3["io_app_id"] = @app_file_history2
      experiment_mock_file_1_op_3["io_logical_time"] = experiment_mock_file_1_op_3["io_logical_time"]+1
      experiment_mock_file_1_op_3["io_timestamp"] = experiment_mock_file_1_op_3["io_timestamp"]+1
      experiment_mock_file_1_op_3.save!

      experiment_mock_file_1_op_4 = experiment_mock_file_1_op_3.dup
      experiment_mock_file_1_op_4["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_1_op_4["io_app_id"] = @app_file_history3
      experiment_mock_file_1_op_4["io_logical_time"] = experiment_mock_file_1_op_4["io_logical_time"]+1
      experiment_mock_file_1_op_4["io_timestamp"] = experiment_mock_file_1_op_4["io_timestamp"]+1
      experiment_mock_file_1_op_4.save!

      experiment_mock_file_1_op_5 = experiment_mock_file_1_op_4.dup
      experiment_mock_file_1_op_5["inode_operation"] = "DELETE"
      experiment_mock_file_1_op_5["io_logical_time"] = experiment_mock_file_1_op_5["io_logical_time"]+1
      experiment_mock_file_1_op_5["io_timestamp"] = experiment_mock_file_1_op_5["io_timestamp"]+1
      experiment_mock_file_1_op_5.save!

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 

      pp "check file history" 
      result = get_file_ops(@project1, 300000, "NONE", "LIST")
      #pp result
      expect(result["items"].length).to eq 5
      expect(result["count"]).to eq 5

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_file_history)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_file_history)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
    end

    it 'timestamp range query' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create mock file history"
      prov_create_experiment(@project1, prov_experiment_id("#{@app1_id}_1"))
      sleep(1)
      prov_create_experiment(@project1, prov_experiment_id("#{@app1_id}_2"))
      sleep(1)
      prov_create_experiment(@project1, prov_experiment_id("#{@app1_id}_3"))
      sleep(1)
      prov_create_experiment(@project1, prov_experiment_id("#{@app1_id}_4"))
      sleep(1)
      prov_create_experiment(@project1, prov_experiment_id("#{@app1_id}_5"))

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 

      pp "check file history" 
      exp1 = get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_1"), false)
      # pp exp1
      exp3 = get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_3"), false)
      # pp exp3
      exp5 = get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_5"), false)
      # pp exp5
      get_ml_in_create_range(@project1, "EXPERIMENT", exp1["createTime"], exp5["createTime"], 3)
      get_ml_in_create_range(@project1, "EXPERIMENT", exp3["createTime"], exp5["createTime"], 1)

      pp "cleanup hops"
      prov_delete_experiment(@project1, prov_experiment_id("#{@app1_id}_1"))
      prov_delete_experiment(@project1, prov_experiment_id("#{@app1_id}_2"))
      prov_delete_experiment(@project1, prov_experiment_id("#{@app1_id}_3"))
      prov_delete_experiment(@project1, prov_experiment_id("#{@app1_id}_4"))
      prov_delete_experiment(@project1, prov_experiment_id("#{@app1_id}_5"))

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_1"), false)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_2"), false)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_3"), false)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_4"), false)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id("#{@app1_id}_5"), false)
    end

    it "search by like file name" do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create experiment"
      prov_create_experiment(@project1, @experiment_app1_name1)
      prov_create_experiment(@project1, @experiment_app2_name1)

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 

      pp "check - ok - search result" 
      experiment1_id = prov_experiment_id(@experiment_app1_name1)
      experiment2_id = prov_experiment_id(@experiment_app2_name1)

      experiments1 = get_ml_asset_like_name(@project1, "EXPERIMENT", @experiment_app1_name1)
      #pp experiment
      expect(experiments1.length).to eq 1
      prov_check_asset_with_id(experiments1, experiment1_id)

      experiments2 = get_ml_asset_like_name(@project1, "EXPERIMENT", @experiment_app2_name1)
      #pp experiment
      expect(experiments2.length).to eq 1
      prov_check_asset_with_id(experiments2, experiment2_id)

      experiments3 = get_ml_asset_like_name(@project1, "EXPERIMENT", @app1_id)
      #pp experiment
      expect(experiments3.length).to eq 1
      prov_check_asset_with_id(experiments3, experiment1_id)      

      experiments4 = get_ml_asset_like_name(@project1, "EXPERIMENT", "application_")
      #pp experiment
      expect(experiments4.length).to eq 2
      prov_check_asset_with_id(experiments4, experiment1_id)
      prov_check_asset_with_id(experiments4, experiment2_id)

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_app1_name1)
      prov_delete_experiment(@project1, @experiment_app2_name1)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_app1_name1)
      experiment_id2 = prov_experiment_id(@experiment_app2_name1)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id2, false)
    end 

    it 'file state pagination' do
      pp "check epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"

      pp "create experiments - pagination"
      prov_create_experiment(@project1, "#{@app1_id}_1")
      prov_create_experiment(@project1, "#{@app1_id}_2")
      prov_create_experiment(@project1, "#{@app1_id}_3")
      prov_create_experiment(@project1, "#{@app1_id}_4")
      prov_create_experiment(@project1, "#{@app1_id}_5")
      prov_create_experiment(@project1, "#{@app1_id}_6")
      prov_create_experiment(@project1, "#{@app1_id}_7")
      prov_create_experiment(@project1, "#{@app1_id}_8")
      prov_create_experiment(@project1, "#{@app1_id}_9")
      prov_create_experiment(@project1, "#{@app1_id}_10")

      pp "wait epipe"
      prov_wait_for_epipe() 

      pp "check experiments pagination" 
      result1 = get_ml_asset_in_project_page(@project1, "EXPERIMENT", false, 0, 7)
      expect(result1["items"].length).to eq 7
      expect(result1["count"]).to eq 10
      result2 = get_ml_asset_in_project_page(@project1, "EXPERIMENT", false, 7, 14)
      expect(result2["items"].length).to eq 3
      expect(result2["count"]).to eq 10

      pp "cleanup hops"
      prov_delete_experiment(@project1, "#{@app1_id}_1")
      prov_delete_experiment(@project1, "#{@app1_id}_2")
      prov_delete_experiment(@project1, "#{@app1_id}_3")
      prov_delete_experiment(@project1, "#{@app1_id}_4")
      prov_delete_experiment(@project1, "#{@app1_id}_5")
      prov_delete_experiment(@project1, "#{@app1_id}_6")
      prov_delete_experiment(@project1, "#{@app1_id}_7")
      prov_delete_experiment(@project1, "#{@app1_id}_8")
      prov_delete_experiment(@project1, "#{@app1_id}_9")
      prov_delete_experiment(@project1, "#{@app1_id}_10")

      pp "check cleanup" 
      prov_wait_for_epipe() 
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)
    end
  end

  describe 'test suite - break project' do
    before :all do
      pp "create project: #{@project1_name}"
      @project1 = create_project_by_name(@project1_name)
    end

    after :all do 
      pp "delete projects"
      delete_project(@project1)
      @project1 = nil
    end
    it 'delete Experiments dataset - check cleanup' do
      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"

      pp "create experiments"
      prov_create_experiment(@project1, @experiment_app1_name1)
      prov_create_experiment(@project1, @experiment_app2_name1)

      pp "wait for epipe"
      prov_wait_for_epipe() 

      pp "check experiments" 
      get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
      get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

      pp "delete Experiments dataset"
      prov_delete_dataset(@project1, "Experiments")

      pp "check experiments" 
      prov_wait_for_epipe() 
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

      pp "cleanup provenance index"
      sleep(1)
      ops = get_file_ops_archival(@project1)
      #pp ops
      cleanup(@project1, ops)

      pp "check provenance index cleanup"
      sleep(1)
      ops = get_file_ops_archival(@project1)
      #pp ops
      expect(ops["count"]).to eq 0
    end
  end

  # describe 'test suite - break project' do
  #   before :all do
  #     pp "create project: #{@project1_name}"
  #     @project1 = create_project_by_name(@project1_name)
  #   end

  #   it 'delete Project - check cleanup' do
  #     pp "restart epipe"
  #     execute_remotely @hostname, "sudo systemctl restart epipe"

  #     pp "create experiments"
  #     prov_create_experiment(@project1, @experiment_app1_name1)
  #     prov_create_experiment(@project1, @experiment_app2_name1)

  #     pp "wait for epipe"
  #     prov_wait_for_epipe() 

  #     pp "check experiments" 
  #     get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #     get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

  #     pp "delete Project"
  #     delete_project(@project1)

  #     pp "check experiments" 
  #     prov_wait_for_epipe() 
  #     #check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #     #check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

  #     pp "cleanup provenance index"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     cleanup(@project1, ops)

  #     pp "check provenance index cleanup"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     expect(ops["count"]).to eq 0
  #     @project1 = nil
  #   end
  # end
end