require File.expand_path('../spec_helper', __FILE__)

describe Gizzard::ForwardingTransformation do
  before do
    @nameserver = stub!.subject
    @forwardings = {
      -100 => 'status_001',
      0 => 'status_002',
      100 => 'status_003'
    }
    @trans = Gizzard::ForwardingTransformation.new(@forwardings)
  end

  describe "apply!" do
    it "creates forwardings with the nameserver" do
      mock(@nameserver).set_forwarding(anything).times(3) do |forwarding|
        table = @forwardings.delete(forwarding.base_id)
        table.should_not be_nil

        forwarding.shard_id.hostname.should == "localhost"
        forwarding.shard_id.table_prefix.should == "#{table}_replicating"
      end
      @trans.apply!(@nameserver)
    end
  end
end


describe Gizzard::Transformation do
  before do
    @nameserver = stub!.subject
    stub(@nameserver).dryrun? { false }

    @config = Gizzard::MigratorConfig.new :prefix => "status", :table_id => 0

    @from_template = make_shard_template 'ReplicatingShard' => [{'BlockedShard' => "SqlShard:host1"}, "SqlShard:host2"]
    @to_template = make_shard_template 'ReplicatingShard' => %w(SqlShard:host2 SqlShard:host3)

    @host_1_template = Gizzard::ShardTemplate.new('SqlShard', 'host1', 1, [])
    @host_2_template = Gizzard::ShardTemplate.new('SqlShard', 'host2', 1, [])
    @host_3_template = Gizzard::ShardTemplate.new('SqlShard', 'host3', 1, [])

    @host_1_id = Gizzard::Thrift::ShardId.new(@host_1_template.host, 'status_001')
    @host_1_info = Gizzard::Thrift::ShardInfo.new(@host_1_id, @host_1_template.type, "", "", 0)
    @host_2_id = Gizzard::Thrift::ShardId.new(@host_2_template.host, 'status_001')
    @host_2_info = Gizzard::Thrift::ShardInfo.new(@host_2_id, @host_2_template.type, "", "", 0)
    @host_3_id = Gizzard::Thrift::ShardId.new(@host_3_template.host, 'status_001')
    @host_3_info = Gizzard::Thrift::ShardInfo.new(@host_3_id, @host_3_template.type, "", "", 0)

    @trans = Gizzard::Transformation.new(@from_template, @to_template, %w(status_001), @config)
  end

  describe "prepare!" do
    it "applies prepare operations to the given nameserver" do
      jobs = [[:create_shard, @host_1_template, nil], [:add_link, @to_template, @host_1_template]]
      mock(@trans).operations { {:prepare => jobs} }
      jobs.each {|job| mock(@trans).apply_job(job, @nameserver) }

      @trans.prepare!(@nameserver)
    end
  end

  describe "copy!" do
    it "applies copy operations to the given nameserver" do
      jobs = [[:copy_shard, @host_1_template, @host_2_template]]
      mock(@trans).operations { {:copy => jobs} }
      jobs.each {|job| mock(@trans).apply_job(job, @nameserver) }

      @trans.copy!(@nameserver)
    end

  end

  describe "wait_for_copies" do
    it "blocks until the nameserver marks all copy destinations as not busy" do
      jobs = [[:copy_shard, @host_1_template, @host_2_template]]
      mock(@trans).operations { {:copy => jobs} }

      busy_responses = [true, false]
      info = stub!.busy? { busy_responses.shift }
      mock(@nameserver).get_shard(@host_2_id) { info }.times(2)

      @trans.wait_for_copies(@nameserver)
    end
  end

  describe "cleanup!" do
    it "applies cleanup operations to the given nameserver" do
      jobs = [[:delete_shard, @host_1_template, nil], [:remove_link, @to_template, @host_1_template]]
      mock(@trans).operations { {:cleanup => jobs} }
      jobs.each {|job| mock(@trans).apply_job(job, @nameserver) }

      @trans.cleanup!(@nameserver)
    end
  end


  # internal method unit tests

  describe "operations" do
    it "needs integration test coverage"
  end

  describe "collapse_jobs" do
    it "does not cancel out an add_link and remove_link with two sets of shards" do
      jobs = [[:add_link, @host_1_template, @host_3_template], [:remove_link, @host_1_template, @host_2_template]]
      @trans.collapse_jobs(jobs).length.should == 2
    end

    it "does not cancel out a create_shard and a delete_shard with two different shards" do
      @trans.collapse_jobs([[:create_shard, @host_1_template, nil], [:delete_shard, @host_2_template, nil]]).length.should == 2
    end

    it "cancels out an add_link and remove_link with the same shards" do
      jobs = [[:add_link, @host_1_template, @host_2_template], [:remove_link, @host_1_template, @host_2_template]]
      @trans.collapse_jobs(jobs).should be_empty
    end

    it "cancels out a create_shard and delete_shard of the same shard" do
      @trans.collapse_jobs([[:create_shard, @host_1_template, nil], [:delete_shard, @host_1_template, nil]]).should be_empty
    end

    it "does not factor in shard children in shard equality" do
      @trans.collapse_jobs([[:create_shard, @from_template, nil], [:delete_shard, @to_template, nil]]).should be_empty
    end

    it "removes job pairs that are the inverse of each other" do
      @trans.collapse_jobs(@trans.create_tree(@from_template) + @trans.destroy_tree(@from_template)).should be_empty
    end
  end

  describe "expand_create_job" do
    describe "when an :add_link job involves a copy destination" do
      before do
        @trans.copy_destination?(@host_3_template).should == true

        @write_only_wrapper = Gizzard::ShardTemplate.new(:write_only, nil, 0, [@host_3_template])
        @job = [:add_link, @to_template, @host_3_template]
        @ops = @trans.expand_create_job(@job)
      end

      it "puts an :add_link to a write only wrapper in the prepare phase" do
        @ops[:prepare].should == [[:add_link, @to_template, @write_only_wrapper]]
      end

      it "adds the real link and removes the link to the write only wrapper in the cleanup phase" do
        @ops[:cleanup].should ==
          [[:add_link, @to_template, @host_3_template],
           [:remove_link, @to_template, @write_only_wrapper]]
      end
    end

    describe "when a :create_shard job involves a copy destination" do
      before do
        @trans.copy_destination?(@host_3_template).should == true

        @write_only_wrapper = Gizzard::ShardTemplate.new(:write_only, nil, 0, [@host_3_template])
        @job = [:create_shard, @host_3_template]
        @ops = @trans.expand_create_job(@job)
      end

      it "creates the shard, creates the write only wrapper, and adds a link for the two in the prepare phase" do
        @ops[:prepare].should ==
          [[:create_shard, @host_3_template],
           [:create_shard, @write_only_wrapper],
           [:add_link, @write_only_wrapper, @host_3_template]]
      end

      it "adds a copy job to the copy phase" do
        @ops[:copy].should == [[:copy_shard, @trans.copy_source, @host_3_template]]
      end

      it "deletes the write only wrapper and the link between the WO wrapper and the shard in the cleanup phase" do
        @ops[:cleanup].should ==
          [[:remove_link, @write_only_wrapper, @host_3_template],
           [:delete_shard, @write_only_wrapper]]
      end
    end

    it "puts the job directly in the prepare phase if it does not involve a copy destination" do
      @trans.copy_destination?(@host_2_template).should == false

      job = [:create_shard, @host_2_template, nil]
      @trans.expand_create_job(job)[:prepare].should == [job]
      @trans.expand_create_job(job)[:cleanup].should == []

      job = [:add_link, @to_template, @host_2_template]
      @trans.expand_create_job(job)[:prepare].should == [job]
      @trans.expand_create_job(job)[:cleanup].should == []
    end
  end

  describe "expand_delete_job" do
    it "puts the job in the cleanup phase if it involves a copy_source" do
      @trans.copy_source?(@host_2_template).should == true

      job = [:delete_shard, @host_2_template]
      @trans.expand_delete_job(job)[:cleanup].should == [job]
      @trans.expand_delete_job(job)[:prepare].should == []

      job = [:remove_link, @from_template, @host_2_template]
      @trans.expand_delete_job(job)[:cleanup].should == [job]
      @trans.expand_delete_job(job)[:prepare].should == []
    end

    it "puts the job in the prepare phase if it does not involve a copy_source" do
      @trans.copy_source?(@host_1_template).should == false

      job = [:delete_shard, @host_1_template]
      @trans.expand_delete_job(job)[:prepare].should == [job]
      @trans.expand_delete_job(job)[:cleanup].should == []

      job = [:remove_link, @from_template, @host_1_template]
      @trans.expand_delete_job(job)[:prepare].should == [job]
      @trans.expand_delete_job(job)[:cleanup].should == []
    end
  end

  describe "sort_jobs" do
    it "sorts jobs based on type priority" do
      sorted_types = [:remove_link, :delete_shard, :create_shard, :add_link, :copy_shard]
      jobs = sorted_types.sort_by { rand }.map {|t| [t, nil, nil] }
      @trans.sort_jobs(jobs).map {|j| j.first }.should == sorted_types
    end
  end

  describe "apply_job" do
    it "schedules a copy when the type is :copy_shard" do
      mock(@nameserver).copy_shard(@host_1_id, @host_2_id)
      @trans.apply_job([:copy_shard, @host_1_template, @host_2_template], @nameserver)
    end

    it "adds a link when the type is :add_link" do
      mock(@nameserver).add_link(@host_1_id, @host_2_id, @host_2_template.weight)
      @trans.apply_job([:add_link, @host_1_template, @host_2_template], @nameserver)
    end

    it "removes a link when the type is :remove_link" do
      mock(@nameserver).remove_link(@host_1_id, @host_2_id)
      @trans.apply_job([:remove_link, @host_1_template, @host_2_template], @nameserver)
    end

    it "creates a shard when the type is :create_shard" do
      mock(@nameserver).create_shard(@host_1_info)
      @trans.apply_job([:create_shard, @host_1_template, nil], @nameserver)
    end

    it "deletes a shard when the type is :delete_shard" do
      mock(@nameserver).delete_shard(@host_1_id)
      @trans.apply_job([:delete_shard, @host_1_template, nil], @nameserver)
    end
  end

    describe "each_shard" do
      it "executes the given block for each shard_enum, with @current_shard_enum set during each run" do
        ids = %w(status_001 status_002 status_003)
        @trans = Gizzard::Transformation.new(nil, nil, ids)
        looped_ids = []

        @trans.each_shard do
          looped_ids << @trans.id(@host_1_template).table_prefix
        end
      end
    end

    describe "id" do
      it "returns a materialized ShardId for the given template" do
        @trans.each_shard do
          @trans.id(@host_1_template).should == @host_1_id
        end
      end
    end

    describe "info" do
      it "returns a materialized ShardInfo for the given template" do
        @trans.each_shard do
          @trans.info(@host_1_template).should == @host_1_info
        end
      end
    end

  describe "copy_destination?" do
    it "returns true if the given template is not a member of the from_template" do
      @trans.copy_destination?(@host_3_template).should == true
    end

    it "returns false when there is no from_template (completely new shards, no data to copy)" do
      @trans = Gizzard::Transformation.new(nil, @to_template, %w(status_001))
      @trans.copy_destination?(@host_1_template).should == false
      @trans.copy_destination?(@host_2_template).should == false
      @trans.copy_destination?(@host_3_template).should == false
    end

    it "returns false if the given template is a member of the from_template (therefore has source data)" do
      @trans.copy_destination?(@host_1_template).should == false
      @trans.copy_destination?(@host_2_template).should == false
    end

    it "returns false if the given template is not concrete" do
      @trans.copy_destination?(@to_template).should == false
    end
  end

  describe "copy_source?" do
    before do
      stub(@trans).copy_source { @host_2_template }
    end

    it "returns true if the given template is the copy source" do
      @trans.copy_source?(@trans.copy_source).should == true
    end

    it "returns true if the given template is not concrete, but a child is the copy source" do
      @trans.copy_source?(@from_template).should == true
    end

    it "returns false if there is no from template (no shards available as sources)" do
      @trans = Gizzard::Transformation.new(nil, @to_template, %w(status_001), @config)
      @trans.copy_source?(@host_1_template).should == false
      @trans.copy_source?(@host_2_template).should == false
      @trans.copy_source?(@host_3_template).should == false
    end

    it "returns false if the given template is not a descendant of from_template (therefore does not have source data)" do
      pending "move test to ShardTemplate"
      @trans.copy_source?(@host_3_template).should == false
    end

    it "returns false if the given template is behind a shard barrier in the from_template" do
      pending "move test to ShardTemplate"
      @trans.copy_source?(@host_1_template).should == false
    end
  end

  describe "add_link" do
    it "returns an add_link message to send to the nameserver" do
      @trans.add_link(@to_template, @host_1_template).should == [:add_link, @to_template, @host_1_template]
    end
  end

  describe "remove_link" do
    it "returns an remove_link message to send to the nameserver" do
      @trans.remove_link(@to_template, @host_1_template).should == [:remove_link, @to_template, @host_1_template]
    end
  end

  describe "create_shard" do
    it "returns an create_shard message to send to the nameserver" do
      @trans.create_shard(@host_1_template).should == [:create_shard, @host_1_template]
    end
  end

  describe "delete_shard" do
    it "returns an delete_shard message to send to the nameserver" do
      @trans.delete_shard(@host_1_template).should == [:delete_shard, @host_1_template]
    end
  end

  describe "create_tree" do
    it "returns a list of messages to send the nameserver that creates a shard and all descendants" do
      jobs = [@trans.create_shard(@to_template)]

      @to_template.children.each do |child|
        jobs << @trans.create_shard(child)
        jobs << @trans.add_link(@to_template, child)
      end

      expected = jobs.map {|(t, a1, a2)| [t.to_s, a1, a2] }
      actual = @trans.create_tree(@to_template).map {|(t, a1, a2)| [t.to_s, a1, a2] }
      actual.sort.should == expected.sort
    end
  end

  describe "destroy_tree" do
    it "returns a list of messages to send the nameserver that destroys a shard and all descendants" do
      jobs = [@trans.delete_shard(@to_template)]

      @to_template.children.each do |child|
        jobs << @trans.delete_shard(child)
        jobs << @trans.remove_link(@to_template, child)
      end

      expected = jobs.map {|(t, a1, a2)| [t.to_s, a1, a2] }
      actual = @trans.destroy_tree(@to_template).map {|(t, a1, a2)| [t.to_s, a1, a2] }
      actual.sort.should == expected.sort
    end
  end
end
