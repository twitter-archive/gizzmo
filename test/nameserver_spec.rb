require File.expand_path('../spec_helper', __FILE__)

describe Gizzard::Nameserver do
  before do
    @client = Object.new
    @second_client = Object.new
    @nameserver = Gizzard::Nameserver.new("localhost:1234", "localhost:4567")
    stub(@nameserver).create_client(anything) do |host|
      { "localhost:1234" => @client, "localhost:4567" => @second_client }[host]
    end
  end

  describe "initialize" do
    it "takes a list of hosts and options" do
      n = Gizzard::Nameserver.new("localhost:1234")
      n.hosts.should == ["localhost:1234"]

      n = Gizzard::Nameserver.new("localhost:1234", "localhost:4567", :dry_run => true)
      n.hosts.should == ["localhost:1234", "localhost:4567"]
    end

    it "takes a :dry_run option that defaults to false" do
      n = Gizzard::Nameserver.new("localhost:1234", :dry_run => true)
      n.dryrun.should == true

      n = Gizzard::Nameserver.new("localhost:1234")
      n.dryrun.should == false
    end

    it "takes a :log option that defaults to '/tmp/gizzmo.log'" do
      n = Gizzard::Nameserver.new("localhost:1234", :log => "/path/to/logfile")
      n.logfile.should == "/path/to/logfile"

      n = Gizzard::Nameserver.new("localhost:1234")
      n.logfile.should == "/tmp/gizzmo.log"
    end
  end

  describe "get_all_links" do
    it "works..."
  end

  describe "get_all_shards" do
    it "works..."
  end

  describe "reload_forwardings" do
    it "reloads the forwardings on every app server" do
      mock(@client).reload_forwardings
      mock(@second_client).reload_forwardings
      @nameserver.reload_forwardings
    end
  end
end

describe Gizzard::Nameserver::Manifest do
  before do
    @shardinfos = [Gizzard::ShardInfo.new(Gizzard::ShardId.new("localhost", "tbl_001_rep"),
                                          "ReplicatingShard", "", "", 0),
                   Gizzard::ShardInfo.new(Gizzard::ShardId.new("sqlhost", "tbl_001"),
                                          "SqlShard", "int", "int", 0)]

    @links = [Gizzard::LinkInfo.new(Gizzard::ShardId.new("localhost", "tbl_001_rep"),
                                    Gizzard::ShardId.new("sqlhost", "tbl_001"),
                                    1)]

    @forwardings = [Gizzard::Forwarding.new(0, 0, Gizzard::ShardId.new("localhost", "tbl_001_rep"))]

    @nameserver = Gizzard::Nameserver.new("localhost:1234")
    mock(@nameserver).get_forwardings { @forwardings }
    mock(@nameserver).get_all_links(@forwardings) { @links }
    mock(@nameserver).get_all_shards { @shardinfos }
  end

  it "memoizes the forwardings list" do
    @nameserver.manifest.forwardings.should == @forwardings
  end

  it "creates a links hash in the form of up_id => [[down_id, weight]]" do
    @nameserver.manifest.links.should == {
      Gizzard::ShardId.new("localhost", "tbl_001_rep") => [[Gizzard::ShardId.new("sqlhost", "tbl_001"), 1]]
    }
  end

  it "creates a shard_infos hash in the form of shard_id => shard_info" do
    @nameserver.manifest.shard_infos.should == {
      Gizzard::ShardId.new("localhost", "tbl_001_rep") =>
        Gizzard::ShardInfo.new(Gizzard::ShardId.new("localhost", "tbl_001_rep"), "ReplicatingShard", "", "", 0),
      Gizzard::ShardId.new("sqlhost", "tbl_001") =>
        Gizzard::ShardInfo.new(Gizzard::ShardId.new("sqlhost", "tbl_001"), "SqlShard", "int", "int", 0)
    }
  end

  it "creates a trees hash in the form of forwarding => shard tree" do
    child  = Gizzard::Shard.new(
      Gizzard::ShardInfo.new(Gizzard::ShardId.new("sqlhost", "tbl_001"), "SqlShard", "int", "int", 0),
      [], 1)
    parent = Gizzard::Shard.new(
      Gizzard::ShardInfo.new(Gizzard::ShardId.new("localhost", "tbl_001_rep"), "ReplicatingShard", "", "", 0),
      [child], 1)

    @nameserver.manifest.trees.should == {
      Gizzard::Forwarding.new(0, 0, Gizzard::ShardId.new("localhost", "tbl_001_rep")) => parent
    }
  end

  it "creates a templates hash om the form of template => [forwarding]" do
    child  = Gizzard::ShardTemplate.new("SqlShard", "sqlhost", 1, "int", "int", [])
    parent = Gizzard::ShardTemplate.new("ReplicatingShard", "localhost", 1, "", "", [child])

    @nameserver.manifest.templates.should == {
      parent => [Gizzard::Forwarding.new(0, 0, Gizzard::ShardId.new("localhost", "tbl_001_rep"))]
    }
  end
end
