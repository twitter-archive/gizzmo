require File.expand_path('../spec_helper.rb', __FILE__)

describe "gizzmo (cli)" do
  def gizzmo(cmd)
    `cd #{ROOT_DIR} && ruby -rubygems -Ilib bin/gizzmo -H localhost -P #{MANAGER_PORT} #{cmd} 2>&1`
  end

  def nameserver
    @nameserver ||= read_nameserver_db
  end

  before do
    reset_nameserver
    @nameserver = nil
  end

  def id(h,p); Gizzard::Thrift::ShardId.new(h,p) end
  def shard(id,c,s = "",d = "",b = 0); Gizzard::Thrift::ShardInfo.new(id,c,s,d,b) end
  def link(p,c,w); Gizzard::Thrift::LinkInfo.new(p,c,w) end
  def forwarding(t,b,s); Gizzard::Thrift::Forwarding.new(t,b,s) end
  def host(h,p,c,s = 0); Gizzard::Thrift::Host.new(h,p,c,s) end

  describe "create" do
    it "creates a single shard" do
      gizzmo "create TestShard localhost/t0_0"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard")]
    end

    it "creates multiple shards" do
      gizzmo "create TestShard localhost/t0_0 localhost/t0_1"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard"),
                                     shard(id("localhost", "t0_1"), "TestShard")]
    end

    it "honors source and destination types" do
      gizzmo "create TestShard -s int -d long localhost/t0_0"
      gizzmo "create TestShard --source-type=int --destination-type=long localhost/t0_1"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard", "int", "long"),
                                     shard(id("localhost", "t0_1"), "TestShard", "int", "long")]
    end
  end

  describe "delete" do
    it "deletes a shard" do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "delete localhost/t0_0"

      nameserver[:shards].should == []
    end
  end

  describe "wrap/unwrap" do
    before do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "create ReplicatingShard localhost/t0_0_replicating"
      gizzmo "addlink localhost/t0_0_replicating localhost/t0_0 1"

      gizzmo "wrap BlockedShard localhost/t0_0"
    end

    it "wrap wraps a shard" do
      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard"),
                                     shard(id("localhost", "t0_0_blocked"), "BlockedShard"),
                                     shard(id("localhost", "t0_0_replicating"), "ReplicatingShard")]

      nameserver[:links].should == [link(id("localhost", "t0_0_blocked"), id("localhost", "t0_0"), 1),
                                    link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0_blocked"), 1)]
    end

    it "unwrap unwraps a shard" do
      gizzmo "unwrap localhost/t0_0_blocked"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard"),
                                     shard(id("localhost", "t0_0_replicating"), "ReplicatingShard")]

      nameserver[:links].should == [link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0"), 1)]
    end

    it "unwrap doesn't unwrap a top level shard or a leaf" do
      gizzmo "unwrap localhost/t0_0"
      gizzmo "unwrap localhost/t0_0_replicating"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard"),
                                     shard(id("localhost", "t0_0_blocked"), "BlockedShard"),
                                     shard(id("localhost", "t0_0_replicating"), "ReplicatingShard")]

      nameserver[:links].should == [link(id("localhost", "t0_0_blocked"), id("localhost", "t0_0"), 1),
                                    link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0_blocked"), 1)]
    end
  end

  describe "rebalance" do
    it "works"
  end

  describe "repair" do
    it "works"
  end

  describe "pair" do
    it "works"
  end

  describe "subtree" do
    it "works"
  end

  describe "markbusy" do
    it "marks shards busy" do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "markbusy localhost/t0_0"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard", "", "", 1)]
    end
  end

  describe "markunbusy" do
    it "marks shards as not busy" do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "markbusy localhost/t0_0"
      gizzmo "markunbusy localhost/t0_0"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard", "", "", 0)]
    end
  end

  describe "hosts" do
    it "works"
  end

  describe "addforwarding" do
    it "adds a forwarding" do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "addforwarding 0 0 localhost/t0_0"

      nameserver[:shards].should      == [shard(id("localhost", "t0_0"), "TestShard")]
      nameserver[:forwardings].should == [forwarding(0, 0, id("localhost", "t0_0"))]
    end
  end

  describe "deleteforwarding" do
    it "removes a forwarding" do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "addforwarding 0 0 localhost/t0_0"
      gizzmo "deleteforwarding 0 0 localhost/t0_0"

      nameserver[:shards].should      == [shard(id("localhost", "t0_0"), "TestShard")]
      nameserver[:forwardings].should == []
    end
  end

  describe "currentforwarding" do
    it "works"
  end

  describe "forwardings" do
    it "works"
  end

  describe "find" do
    it "works"
  end

  describe "links" do
    it "works"
  end

  describe "info" do
    it "works"
  end

  describe "reload" do
    it "works"
  end

  describe "drill" do
    it "works"
  end

  describe "addlink" do
    it "links two shards" do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "create ReplicatingShard localhost/t0_0_replicating"
      gizzmo "addlink localhost/t0_0_replicating localhost/t0_0 1"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard"),
                                     shard(id("localhost", "t0_0_replicating"), "ReplicatingShard")]

      nameserver[:links].should == [link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0"), 1)]
    end
  end

  describe "unlink" do
    it "unlinks two shards" do
      gizzmo "create TestShard localhost/t0_0"
      gizzmo "create ReplicatingShard localhost/t0_0_replicating"
      gizzmo "addlink localhost/t0_0_replicating localhost/t0_0 1"
      gizzmo "unlink localhost/t0_0_replicating localhost/t0_0"

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard"),
                                     shard(id("localhost", "t0_0_replicating"), "ReplicatingShard")]

      nameserver[:links].should == []
    end
  end

  describe "report" do
    it "works"
  end

  describe "lookup" do
    it "works"
  end

  describe "copy" do
    it "works"
  end

  describe "busy" do
    it "works"
  end

  describe "setup-migrate" do
    it "works"
  end

  describe "finish-migrate" do
    it "works"
  end

  describe "inject" do
    it "works"
  end

  describe "flush" do
    it "works"
  end

  describe "add-host" do
    it "creates single and multiple hosts" do
      gizzmo("add-host c1:c1host1:7777")
      gizzmo("add-host c2:c2host1:7777 c2:c2host2:7777")

      nameserver[:hosts].should == [ host("c1host1", 7777, "c1"),
                                     host("c2host1", 7777, "c2"),
                                     host("c2host2", 7777, "c2") ]
    end
  end

  describe "remove-host" do
    it "creates single and multiple hosts" do
      gizzmo("add-host c1:c1host1:7777")
      gizzmo("remove-host c1:c1host1:7777")

      nameserver[:hosts].should == []
    end
  end

  describe "list-hosts" do
    it "returns a list of all hosts and their status" do
      gizzmo("add-host c1:c1host1:7777 c2:c2host1:7777 c2:c2host2:7777")

      gizzmo("list-hosts").should == <<-EOF
c1:c1host1:7777 0
c2:c2host1:7777 0
c2:c2host2:7777 0
      EOF
    end
  end
end
