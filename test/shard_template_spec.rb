require File.expand_path('../spec_helper', __FILE__)

describe Gizzard::ShardTemplate do
  before do
    @sql         = Gizzard::ShardTemplate.new("SqlShard", "sqlhost", 1, "", "", [])
    @sql2        = Gizzard::ShardTemplate.new("SqlShard", "sqlhost2", 1, "", "", [])
    @blocked     = Gizzard::ShardTemplate.new("BlockedShard", "", 1, "", "", [@sql])
    @replicating = Gizzard::ShardTemplate.new("ReplicatingShard", "", 1, "", "", [@blocked, @sql2])
  end

  describe "concrete?" do
    it "is false when a virtual shard type" do
      Gizzard::Shard::VIRTUAL_SHARD_TYPES.each do |type|
        t = Gizzard::ShardTemplate.new(type, "localhost", 1, "", "", [])
        t.should_not be_concrete
      end
    end

    it "is true when not a virtual shard type" do
      @sql.should be_concrete
    end
  end

  describe "host" do
    it "is the template's shard if concrete" do
      @sql.host.should == "sqlhost"
    end

    it "is the childs host if virtual and one child" do
      @blocked.host.should == "sqlhost"
    end

    it "is the abstract host if virtual with more than one child" do
      @replicating.host.should == Gizzard::ShardTemplate::ABSTRACT_HOST
    end
  end

  describe "children" do
    it "returns a sorted list" do
      @replicating.children.should == [@blocked, @sql2].sort {|a, b| b <=> a }
      @replicating.instance_variable_get("@children").reverse!
      @replicating.children.should == [@blocked, @sql2].sort {|a, b| b <=> a }
    end
  end

  describe "comparison methods" do
    describe "shared_host?" do
      it "is true if self and the other template share a descendant concrete identifier" do
        other = Gizzard::ShardTemplate.new("FailingOverShard", "", 1, "", "", [@sql2])

        @sql2.shared_host?(other).should        be_true
        @replicating.shared_host?(other).should be_true
        @blocked.shared_host?(other).should     be_false
      end
    end

    describe "<=>" do
      it "raises if other is not a ShardTemplate" do
        lambda { @sql <=> "foo" }.should raise_error(ArgumentError)
      end
    end

    describe "eql?" do
      it "returns false if other is not a ShardTemplate" do
        @sql.eql?("foo").should be_false
        (@sql == "foo").should be_false
      end

      it "is structural equality" do
        other_replicating = Marshal.load(Marshal.dump(@replicating))
        @replicating.eql?(other_replicating).should be_true
        (@replicating == other_replicating).should be_true
      end
    end
  end

  describe "config methods" do
    describe "to_config" do
      it "returns a human-readable string" do
        @sql.to_config.should == "SqlShard(sqlhost,1)"
        @blocked.to_config.should == 'BlockedShard(1) -> SqlShard(sqlhost,1)'
        @replicating.to_config.should ==
          'ReplicatingShard(1) -> (SqlShard(sqlhost2,1), BlockedShard(1) -> SqlShard(sqlhost,1))'
      end
    end
  end

  describe "config class methods" do
    describe "parse" do
      it "builds a shard template tree" do
        Gizzard::ShardTemplate.parse("SqlShard(sqlhost,2)").should ==
          Gizzard::ShardTemplate.new("SqlShard", "sqlhost", 2, "", "", [])

        Gizzard::ShardTemplate.parse("SqlShard(sqlhost,1,int,int)").should ==
          Gizzard::ShardTemplate.new("SqlShard", "sqlhost", 1, "int", "int", [])

        Gizzard::ShardTemplate.parse(
          'ReplicatingShard -> (SqlShard(sqlhost2,1), BlockedShard -> SqlShard(sqlhost,1))'
        ).should == @replicating
      end
    end
  end
end
