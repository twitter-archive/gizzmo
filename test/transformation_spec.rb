require File.expand_path('../spec_helper', __FILE__)

describe Gizzard::ForwardingTransformation do
  before do
    @nameserver = stub!.subject
    @forwardings = {
      -100 => id('localhost', 'status_001_replicating'),
      0 => id('localhost', 'status_002_replicating'),
      100 => id('localhost', 'status_003_replicating')
    }
    @trans = Gizzard::ForwardingTransformation.new(1, @forwardings)
  end

  describe "apply!" do
    it "creates forwardings with the nameserver" do
      trans = Gizzard::ForwardingTransformation.new(1, @forwardings)

      mock(@nameserver).set_forwarding(anything).times(3) do |forwarding|
        shard_id = @forwardings.delete(forwarding.base_id)
        shard_id.should_not be_nil

        forwarding.table_id.should == 1
        forwarding.shard_id.should == shard_id
      end

      trans.apply!(@nameserver)
    end

    it "uses a table id of 0 if passed nil" do
      trans = Gizzard::ForwardingTransformation.new(nil, @forwardings)

      mock(@nameserver).set_forwarding(anything).times(3) do |forwarding|
        shard_id = @forwardings.delete(forwarding.base_id)
        forwarding.table_id.should == 0
      end

      trans.apply!(@nameserver)
    end
  end
end


describe Gizzard::Transformation do
  Op = Gizzard::Transformation::Op

  def create_shard(t);      Op::CreateShard.new(mk_template(t)) end
  def delete_shard(t);      Op::DeleteShard.new(mk_template(t)) end
  def add_link(f, t);       Op::AddLink.new(mk_template(f), mk_template(t)) end
  def remove_link(f, t);    Op::RemoveLink.new(mk_template(f), mk_template(t)) end
  def copy_shard(f, t);     Op::CopyShard.new(mk_template(f), mk_template(t)) end
  def set_forwarding(t);    Op::SetForwarding.new(mk_template(t)) end
  def remove_forwarding(t); Op::RemoveForwarding.new(mk_template(t)) end

  before do
    @nameserver = stub!.subject
    stub(@nameserver).dryrun? { false }

    @config = Gizzard::MigratorConfig.new :prefix => "status", :table_id => 0

    @from_template = mk_template 'ReplicatingShard' => [{'BlockedShard' => "SqlShard:host1"}, "SqlShard:host2"]
    @to_template = mk_template 'ReplicatingShard' => %w(SqlShard:host2 SqlShard:host3)

    @blocked_template = mk_template 'BlockedShard' => "SqlShard:host1"
    @host_1_template = mk_template 'SqlShard:host1'
    @host_2_template = mk_template 'SqlShard:host2'
    @host_3_template = mk_template 'SqlShard:host3'

    @host_1_id = Gizzard::ShardId.new(@host_1_template.host, 'status_001')
    @host_1_info = Gizzard::ShardInfo.new(@host_1_id, @host_1_template.type, "", "", 0)
    @host_2_id = Gizzard::ShardId.new(@host_2_template.host, 'status_001')
    @host_2_info = Gizzard::ShardInfo.new(@host_2_id, @host_2_template.type, "", "", 0)
    @host_3_id = Gizzard::ShardId.new(@host_3_template.host, 'status_001')
    @host_3_info = Gizzard::ShardInfo.new(@host_3_id, @host_3_template.type, "", "", 0)

    @trans = Gizzard::Transformation.new(@from_template, @to_template, %w(status_001))
  end

  # internal method tests

  describe "operations" do
    it "does a basic replica addition" do
      from = mk_template 'ReplicatingShard' => ['SqlShard:host1', 'SqlShard:host2']
      to   = mk_template 'ReplicatingShard' => ['SqlShard:host1', 'SqlShard:host2', 'SqlShard:host3']

      Gizzard::Transformation.new(from, to, 'BlockedShard').operations.should == {
        :prepare => [ create_shard('SqlShard:host3'),
                      create_shard('BlockedShard'),
                      add_link('BlockedShard', 'SqlShard:host3'),
                      add_link('ReplicatingShard', 'BlockedShard') ],
        :copy =>    [ copy_shard('SqlShard:host1', 'SqlShard:host3') ],
        :cleanup => [ add_link('ReplicatingShard', 'SqlShard:host3'),
                      remove_link('ReplicatingShard', 'BlockedShard'),
                      remove_link('BlockedShard', 'SqlShard:host3'),
                      delete_shard('BlockedShard') ]
      }
    end

    it "does a partition migration" do
      from = mk_template 'ReplicatingShard' => ['SqlShard:host1', 'SqlShard:host2']
      to   = mk_template 'ReplicatingShard' => ['SqlShard:host3', 'SqlShard:host4']

      Gizzard::Transformation.new(from, to).operations.should == {
        :prepare => [ create_shard('SqlShard:host4'),
                      create_shard('WriteOnlyShard'),
                      create_shard('WriteOnlyShard'),
                      create_shard('SqlShard:host3'),
                      add_link('ReplicatingShard', 'WriteOnlyShard'),
                      add_link('WriteOnlyShard', 'SqlShard:host4'),
                      add_link('WriteOnlyShard', 'SqlShard:host3'),
                      add_link('ReplicatingShard', 'WriteOnlyShard') ],
        :copy =>    [ copy_shard('SqlShard:host1', 'SqlShard:host4'),
                      copy_shard('SqlShard:host1', 'SqlShard:host3') ],
        :cleanup => [ add_link('ReplicatingShard', 'SqlShard:host4'),
                      add_link('ReplicatingShard', 'SqlShard:host3'),
                      remove_link('ReplicatingShard', 'WriteOnlyShard'),
                      remove_link('ReplicatingShard', 'WriteOnlyShard'),
                      remove_link('WriteOnlyShard', 'SqlShard:host3'),
                      remove_link('ReplicatingShard', 'SqlShard:host1'),
                      remove_link('ReplicatingShard', 'SqlShard:host2'),
                      remove_link('WriteOnlyShard', 'SqlShard:host4'),
                      delete_shard('SqlShard:host1'),
                      delete_shard('SqlShard:host2'),
                      delete_shard('WriteOnlyShard'),
                      delete_shard('WriteOnlyShard') ]
      }
    end

    it "migrates the top level shard" do
      from = mk_template 'ReplicatingShard' => ['SqlShard:host1', 'SqlShard:host2']
      to   = mk_template 'FailingOverShard' => ['SqlShard:host1', 'SqlShard:host2']

      Gizzard::Transformation.new(from, to).operations.should == {
        :prepare => [ create_shard('FailingOverShard'),
                      add_link('FailingOverShard', 'SqlShard:host2'),
                      add_link('FailingOverShard', 'SqlShard:host1'),
                      set_forwarding('FailingOverShard'),
                      remove_forwarding('ReplicatingShard'),
                      remove_link('ReplicatingShard', 'SqlShard:host1'),
                      remove_link('ReplicatingShard', 'SqlShard:host2'),
                      delete_shard('ReplicatingShard') ],
        :copy =>    [],
        :cleanup => []
      }
    end

    it "wraps a shard" do
      from = mk_template 'ReplicatingShard' => ['SqlShard:host1', 'SqlShard:host2']
      to   = mk_template 'ReplicatingShard' => [{'ReadOnlyShard' => 'SqlShard:host1'}, 'SqlShard:host2']

      Gizzard::Transformation.new(from, to).operations.should == {
        :prepare => [ create_shard('ReadOnlyShard'),
                      add_link('ReadOnlyShard', 'SqlShard:host1'),
                      add_link('ReplicatingShard', 'ReadOnlyShard'),
                      remove_link('ReplicatingShard', 'SqlShard:host1') ],
        :copy =>    [],
        :cleanup => []
      }
    end

    it "raises an argument error if the transformation requires a copy without a valid source" do
      to = mk_template 'ReplicatingShard' => ['SqlShard:host1', 'SqlShard:host2']

      Gizzard::Shard::INVALID_COPY_TYPES.each do |invalid_type|
        from = mk_template 'ReplicatingShard' => [{invalid_type => 'SqlShard:host1'}]
        lambda { Gizzard::Transformation.new(from, to) }.should raise_error(ArgumentError)
      end
    end
  end

  describe "collapse_jobs" do
    def collapse(jobs); @trans.collapse_jobs(jobs) end

    it "works" do
      jobs = [ Op::AddLink.new(@host_1_template, @host_2_template),
               Op::AddLink.new(@host_1_template, @host_3_template) ]
      collapse(jobs).should == jobs

      collapse([ Op::AddLink.new(@host_1_template, @host_2_template),
                 Op::RemoveLink.new(@host_1_template, @host_2_template) ]).should == []

      collapse([ Op::RemoveLink.new(@host_1_template, @host_2_template),
                 Op::AddLink.new(@host_1_template, @host_2_template) ]).should == []

      collapse(@trans.create_tree(@from_template) + @trans.destroy_tree(@from_template)).should == []

      collapse(@trans.create_tree(@to_template) + @trans.destroy_tree(@from_template)).should ==
        [ Op::CreateShard.new(@host_3_template),
          Op::AddLink.new(@to_template, @host_3_template),
          Op::RemoveLink.new(@blocked_template, @host_1_template),
          Op::DeleteShard.new(@host_1_template),
          Op::RemoveLink.new(@from_template, @blocked_template),
          Op::DeleteShard.new(@blocked_template) ]
    end
  end

  describe "copy_destination?" do
    it "returns true if the given template is not a member of the from_template" do
      @trans.copy_destination?(@host_3_template).should == true
    end

    it "returns false when there is no from_template (completely new shards, no data to copy)" do
      @trans = Gizzard::Transformation.new(nil, @to_template)
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
end
