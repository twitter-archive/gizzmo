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
      Gizzard::ShardTemplate::VIRTUAL_SHARD_TYPES.each do |type|
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
    it "returns a sorted list"
  end

  describe "comparison methods" do
    describe "similar?" do
    end

    describe "<=>" do
    end

    describe "eql?" do
      it "is structural equality" do
        other_replicating = Marshal.load(Marshal.dump(@replicating))
        @replicating.eql?(other_replicating).should be_true
      end
    end
  end

  describe "config methods" do
    describe "to_config" do
      it "returns a ruby structure that coverts to human-readable yaml" do
        @sql.to_config.should == "SqlShard:sqlhost:1"
        @blocked.to_config.should == { "BlockedShard:1" => "SqlShard:sqlhost:1" }
        @replicating.to_config.should == {
          "ReplicatingShard:1" => [
            "SqlShard:sqlhost2:1",
            { "BlockedShard:1" => "SqlShard:sqlhost:1" }
          ]
        }
      end
    end
  end

  describe "config class methods" do
    describe "from_config" do
      it "builds a shard template tree" do
        Gizzard::ShardTemplate.from_config("SqlShard:sqlhost:1").should ==
          Gizzard::ShardTemplate.new("SqlShard", "sqlhost", 1, "", "", [])

        opts = {:source_type => "int", :dest_type => "int"}
        Gizzard::ShardTemplate.from_config("SqlShard:sqlhost:1", opts).should ==
          Gizzard::ShardTemplate.new("SqlShard", "sqlhost", 1, "int", "int", [])

        Gizzard::ShardTemplate.from_config(
          "ReplicatingShard:1" => [ "SqlShard:sqlhost2:1", { "BlockedShard:1" => "SqlShard:sqlhost:1" } ]
        ).should == @replicating
      end
    end
  end
end
