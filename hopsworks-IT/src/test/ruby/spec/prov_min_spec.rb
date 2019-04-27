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
    check_archive_disabled()
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
  end

  describe 'test suite - 2 projects' do
    before :all do
      pp "create project: #{@project1_name}"
      @project1 = create_project_by_name(@project1_name)
      pp "create project: #{@project2_name}"
      @project2 = create_project_by_name(@project2_name)
    end

    after :each do
      pp "cleanup cycle"
      ops1 = cleanup_cycle(@project1)
      ops2 = cleanup_cycle(@project2)

      if ops1["count"] != 0
        pp "secondary cleanup cycle(1)"
        sleep(1)
        ops1 = cleanup_cycle(@project1)
      end

      if ops2["count"] != 0
        pp "secondary cleanup cycle(2)"
        sleep(1)
        ops2 = cleanup_cycle(@project2)
      end

      expect(ops1["count"]).to eq 0
      expect(ops2["count"]).to eq 0
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
    
#     after :each do
#       pp "cleanup cycle"
#       ops = cleanup_cycle(@project1)
#
#       #pp ops
#       if ops["count"] != 0
#         pp "secondary cleanup cycle"
#         sleep(1)
#         ops = cleanup_cycle(@project1)
#       end
#       expect(ops["count"]).to eq 0
#     end
#
#     after :all do
#       pp "delete projects"
#       delete_project(@project1)
#       @project1 = nil
#     end

    describe 'experiments' do
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
        #pp result1
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

      it 'experiment with app_id as xattr' do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "create experiment with xattr"
        AppProv.create(id: @app1_id, state: "FINISHED", timestamp:5, name:@app1_id, user:"my_user", submit_time:1,
        start_time:2, finish_time:4)
        prov_create_experiment(@project1, @experiment_app1_name1)
        attach_app_id_xattr(@project1, @experiment_app1_name1, @app1_id)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"

        pp "check experiment"
        prov_wait_for_epipe()
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT", true, 1)["items"]
        pp result1
        xattrsExact = Hash.new
        xattrsExact["app_id"] = @app1_id
        prov_check_asset_with_xattrs(result1, prov_experiment_id(@experiment_app1_name1), xattrsExact)

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)

        pp "check hops cleanup"
        prov_wait_for_epipe()
        get_ml_asset_in_project(@project1, "EXPERIMENT", false, 0)
      end
      it 'experiment - app expansion - no app id' do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "setup"
        project = @project1
        #no app id
        prov_create_experiment(project, @experiment_app1_name1)
        #app id attached as xattr
        prov_create_experiment(project, @experiment_app2_name1)
        experiment2_record = prov_get_experiment_record(project, @experiment_app2_name1)
        expect(experiment2_record.length).to eq 1
        prov_add_xattr(experiment2_record[0], "app_id", @app2_id, "XATTR_ADD", 1)
        #setup fake app
        user_name = experiment2_record[0]["io_user_name"]
        prov_add_app_states2(@app2_id, user_name)
        #app id (tls) created by app
        prov_create_experiment(project, @experiment_app3_name1)
        experiment3_record = prov_get_experiment_record(project, @experiment_app3_name1)
        expect(experiment3_record.length).to eq 1
        experiment3_record[0]["io_app_id"] = @app3_id
        pp experiment3_record[0]
        experiment3_record[0].save!
        #setup fake app
        user_name = experiment3_record[0]["io_user_name"]
        prov_add_app_states2(@app3_id, user_name)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"
        prov_wait_for_epipe()

        pp "test query"
        query = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state?filter_by=ML_TYPE:EXPERIMENT&expand=APP"
        pp "#{query}"
        result = get "#{query}"
        expect_status(200)
        parsed_result = JSON.parse(result)
        expect(parsed_result["count"]).to eq 3
        pp parsed_result
        prov_check_experiment3(parsed_result["items"], @experiment_app1_name1, "UNKNOWN")
        prov_check_experiment3(parsed_result["items"], @experiment_app2_name1, "FINISHED")
        prov_check_experiment3(parsed_result["items"], @experiment_app3_name1, "FINISHED")

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app2_name1)
        prov_delete_experiment(@project1, @experiment_app3_name1)

        pp "check hops cleanup"
        prov_wait_for_epipe()
        get_ml_asset_in_project(@project1, "EXPERIMENT", false, 0)
      end

      it 'experiment - sort xattr - string and number', focus: true do
        pp "setup"
        project = @project1
        prov_create_experiment(project, @experiment_app1_name1)
        experiment1_record = prov_get_experiment_record(project, @experiment_app1_name1)
        expect(experiment1_record.length).to eq 1
        prov_add_xattr(experiment1_record[0], "xattr_string", "some text", "XATTR_ADD", 1)
        prov_add_xattr(experiment1_record[0], "xattr_long", 24, "XATTR_ADD", 2)
        jsonVal = JSON['{"xattr_string":"some other text","xattr_long": 12}']
        prov_add_xattr(experiment1_record[0], "xattr_json", JSON[jsonVal], "XATTR_ADD", 3)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"
        prov_wait_for_epipe()

        pp "check mapping"
        query = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/index/mapping"
        pp "#{query}"
        result = get "#{query}"
        expect_status(200)
        parsed_result = JSON.parse(result)
        #pp parsed_result["mapping"]["entry"]
        e1 = parsed_result["mapping"]["entry"].select { |e| e["key"] == "xattr_prov.xattr_string.raw" }
        expect(e1[0]["value"]).to eq "text"
        e2 = parsed_result["mapping"]["entry"].select { |e| e["key"] == "xattr_prov.xattr_long.value" }
        expect(e2[0]["value"]).to eq "long"
        e3 = parsed_result["mapping"]["entry"].select { |e| e["key"] == "xattr_prov.xattr_json.value.xattr_long" }
        expect(e3[0]["value"]).to eq "long"
        e4 = parsed_result["mapping"]["entry"].select { |e| e["key"] == "xattr_prov.xattr_json.value.xattr_string" }
        expect(e4[0]["value"]).to eq "text"

        pp "query with sort"
        query = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state?xattr_sort_by=xattr_string:ASC"
        pp "#{query}"
        result = get "#{query}"
        expect_status(200)

        query = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state?xattr_sort_by=xattr_long:ASC"
        pp "#{query}"
        result = get "#{query}"
        expect_status(200)

        query = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state?xattr_sort_by=xattr_json.xattr_long:ASC"
        pp "#{query}"
        result = get "#{query}"
        expect_status(200)

        query = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state?xattr_sort_by=xattr_json.xattr_string:ASC"
        pp "#{query}"
        result = get "#{query}"
        expect_status(200)
      end
    end
  end
end