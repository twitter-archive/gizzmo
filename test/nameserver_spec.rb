require File.expand_path('../spec_helper', __FILE__)

describe Gizzard::Shard do
  describe "parse_enumeration" do
    it "parses correctly" do
      Gizzard::Shard.parse_enumeration("edges_backwards_1_003_a").should == 3
      Gizzard::Shard.parse_enumeration("status_0345").should == 345
    end
  end

  describe "canonical_shard_id_map" do
    it "returns a map of canonical to actual shard table prefixes" do
      s = Gizzard::Shard.new(info("localhost", "t0_001_replicating", "ReplicatingShard"),
                             [Gizzard::Shard.new(info("localhost", "shard_0001", "SqlShard"), [], 1)],
                             1)
      s.canonical_shard_id_map.should == {
        id("localhost", "shard_0001_replicating") => id("localhost", "t0_001_replicating"),
        id("localhost", "shard_0001") => id("localhost", "shard_0001")
      }

      s.canonical_shard_id_map("edges", -2).should == {
        id("localhost", "edges_n2_0001_replicating") => id("localhost", "t0_001_replicating"),
        id("localhost", "edges_n2_0001") => id("localhost", "shard_0001")
      }

      s.canonical_shard_id_map("groups", 0, 3).should == {
        id("localhost", "groups_0_0003_replicating") => id("localhost", "t0_001_replicating"),
        id("localhost", "groups_0_0003") => id("localhost", "shard_0001")
      }
    end
  end
end

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

  describe "reload_config" do
    it "reloads config on every app server" do
      mock(@client).reload_config
      mock(@second_client).reload_config
      @nameserver.reload_config
    end
  end
end

describe Gizzard::Nameserver::Manifest do
  before do
    @shardinfos = [info("localhost", "tbl_001_rep", "ReplicatingShard", "", "", 0),
                   info("sqlhost", "tbl_001", "SqlShard", "int", "int", 0)]

    @links = [link(id("localhost", "tbl_001_rep"), id("sqlhost", "tbl_001"), 1)]

    @forwardings = [forwarding(0, 0, id("localhost", "tbl_001_rep"))]

    @nameserver = Gizzard::Nameserver.new("localhost:1234")
    @state = Object.new

    mock(@nameserver).dump_nameserver([0]) { [@state] }
    mock(@state).forwardings { @forwardings }
    mock(@state).links { @links }
    mock(@state).shards { @shardinfos }
  end

  it "memoizes the forwardings list" do
    @nameserver.manifest(0).forwardings.should == @forwardings
  end

  it "creates a links hash in the form of up_id => [[down_id, weight]]" do
    @nameserver.manifest(0).links.should == {
      id("localhost", "tbl_001_rep") => [[id("sqlhost", "tbl_001"), 1]]
    }
  end

  it "creates a shard_infos hash in the form of shard_id => shard_info" do
    @nameserver.manifest(0).shard_infos.should == {
      id("localhost", "tbl_001_rep") => info("localhost", "tbl_001_rep", "ReplicatingShard", "", "", 0),
      id("sqlhost", "tbl_001")       => info("sqlhost", "tbl_001", "SqlShard", "int", "int", 0)
    }
  end

  it "creates a trees hash in the form of forwarding => shard tree" do
    child  = Gizzard::Shard.new(info("sqlhost", "tbl_001", "SqlShard", "int", "int", 0), [], 1)
    parent = Gizzard::Shard.new(info("localhost", "tbl_001_rep", "ReplicatingShard", "", "", 0), [child], 1)

    @nameserver.manifest(0).trees.should == {
      forwarding(0, 0, id("localhost", "tbl_001_rep")) => parent
    }
  end

  it "creates a templates hash om the form of template => [forwarding]" do
    child  = Gizzard::ShardTemplate.new("SqlShard", "sqlhost", 1, "int", "int", [])
    parent = Gizzard::ShardTemplate.new("ReplicatingShard", "localhost", 1, "", "", [child])

     @nameserver.manifest(0).templates.should == {
      parent => [forwarding(0, 0, id("localhost", "tbl_001_rep"))]
    }
  end
end

describe Gizzard::Nameserver::CommandLog do
  it "creates and gets a log" do
    name = "test1"
    log_created = ns.command_log(name, true)
    log_gotten = ns.command_log(name, false)
    log_created.log_id.should == log_gotten.log_id
  end

  it "it appends to and peeks at a log" do
    log = ns.command_log("test2", true)
    ["it's", "better than bad", "log!"].map do |entry|
      to = mkTO(entry)
      log.push!(to)
      log.peek(1)[0].command.should == to
    end
  end

  it "it pops from a log, and ignores popped in peek" do
    log = ns.command_log("test3", true)
    entries = ["un", "deux", "trois"]
    entries.each do |entry|
      log.push!(mkTO(entry))
    end
    log.peek(3).reverse.map{|e| e.content}.should == entries
    entries.reverse_each do |entry|
      peeked = log.peek(1)[0]
      peeked.command.should == mkTO(entry)
      log.pop!(peeked.id)
    end
  end

  # makes a simple transform op containing a string
  def mkTO(string)
    TransformOperation.with(:delete_shard, ShardId.new(string, string))
  end
end
