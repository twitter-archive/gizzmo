require File.expand_path('../spec_helper.rb', __FILE__)

describe "gizzmo cmd interface" do
  def gizzmo(cmd)
    `cd #{ROOT_DIR} && ruby -rubygems -Ilib bin/gizzmo -H localhost -P #{MANAGER_PORT} #{cmd}`
  end

  def nameserver
    @nameserver ||= read_nameserver_db
  end

  before do
    reset_database(NAMESERVER_DATABASE)
    @nameserver = nil
  end

  def id(h,p); Gizzard::Thrift::ShardId.new(h,p) end
  def shard(id,c,s,d,b = 0); Gizzard::Thrift::ShardInfo.new(id,c,s,d,b) end
  def link(p,c,w); Gizzard::Thrift::LinkInfo.new(p,c,w) end
  def forwarding(t,b,s); Gizzard::Thrift::Forwarding.new(t,b,s) end
  def host(h,p,c,s = 0); Gizzard::Thrift::Host.new(h,p,c,s) end

  describe "create" do
    it "creates a single shard" do
      gizzmo("create TestShard localhost/t0_0")

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard", "", "")]
    end

    it "creates multiple shards" do
      gizzmo("create TestShard localhost/t0_0 localhost/t0_1")

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard", "", ""),
                                     shard(id("localhost", "t0_1"), "TestShard", "", "")]
    end

    it "honors source and destination types" do
      gizzmo("create TestShard -s int -d long localhost/t0_0")
      gizzmo("create TestShard --source-type=int --destination-type=long localhost/t0_1")

      nameserver[:shards].should == [shard(id("localhost", "t0_0"), "TestShard", "int", "long"),
                                     shard(id("localhost", "t0_1"), "TestShard", "int", "long")]
    end
  end

  describe "wrap" do
  end

  describe "rebalance" do
  end

  describe "repair" do
  end

  describe "pair" do
  end

  describe "subtree" do
  end

  describe "markbusy" do
  end

  describe "markunbusy" do
  end

  describe "hosts" do
  end

  describe "deleteforwarding" do
  end

  describe "delete" do
  end

  describe "addforwarding" do
  end

  describe "currentforwarding" do
  end

  describe "forwardings" do
  end

  describe "unwrap" do
  end

  describe "find" do
  end

  describe "links" do
  end

  describe "info" do
  end

  describe "reload" do
  end

  describe "drill" do
  end

  describe "addlink" do
  end

  describe "unlink" do
  end

  describe "report" do
  end

  describe "lookup" do
  end

  describe "copy" do
  end

  describe "busy" do
  end

  describe "setup-migrate" do
  end

  describe "finish-migrate" do
  end

  describe "inject" do
  end

  describe "flush" do
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
