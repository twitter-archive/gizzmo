require File.expand_path('../spec_helper.rb', __FILE__)

describe "gizzmo (cli)" do
  def nameserver_db
    @nameserver_db ||= read_nameserver_db
  end

  before do
    reset_nameserver
    @nameserver_db = nil
  end

  describe "basic manipulation commands" do
    describe "create" do
      it "creates a single shard" do
        gizzmo "create TestShard localhost/t_0_000"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard")]
      end

      it "creates multiple shards" do
        gizzmo "create TestShard localhost/t_0_000 localhost/t_0_001"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard"),
                                          info("localhost", "t_0_001", "TestShard")]
      end

      it "honors source and destination types" do
        gizzmo "create TestShard -s int -d long localhost/t_0_000"
        gizzmo "create TestShard --source-type=int --destination-type=long localhost/t_0_001"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard", "int", "long"),
                                          info("localhost", "t_0_001", "TestShard", "int", "long")]
      end
    end

    describe "delete" do
      it "deletes a shard" do
        ns.create_shard info("localhost", "t_0_000", "TestShard")

        gizzmo "delete localhost/t_0_000"

        nameserver_db[:shards].should == []
      end
    end

    describe "wrap/unwrap" do
      before do
        ns.create_shard info("localhost", "t_0_000", "TestShard")
        ns.create_shard info("localhost", "t_0_000_replicating", "ReplicatingShard")
        ns.add_link id("localhost", "t_0_000_replicating"), id("localhost", "t_0_000"), 1

        gizzmo "wrap BlockedShard localhost/t_0_000"
      end

      it "wrap wraps a shard" do
        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard"),
                                          info("localhost", "t_0_000_blocked", "BlockedShard"),
                                          info("localhost", "t_0_000_replicating", "ReplicatingShard")]

        nameserver_db[:links].should == [link(id("localhost", "t_0_000_blocked"), id("localhost", "t_0_000"), 1),
                                         link(id("localhost", "t_0_000_replicating"), id("localhost", "t_0_000_blocked"), 1)]
      end

      it "unwrap unwraps a shard" do
        gizzmo "unwrap localhost/t_0_000_blocked"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard"),
                                          info("localhost", "t_0_000_replicating", "ReplicatingShard")]

        nameserver_db[:links].should == [link(id("localhost", "t_0_000_replicating"), id("localhost", "t_0_000"), 1)]
      end

      it "unwrap doesn't unwrap a top level shard or a leaf" do
        gizzmo "unwrap localhost/t_0_000"
        gizzmo "unwrap localhost/t_0_000_replicating"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard"),
                                          info("localhost", "t_0_000_blocked", "BlockedShard"),
                                          info("localhost", "t_0_000_replicating", "ReplicatingShard")]

        nameserver_db[:links].should == [link(id("localhost", "t_0_000_blocked"), id("localhost", "t_0_000"), 1),
                                         link(id("localhost", "t_0_000_replicating"), id("localhost", "t_0_000_blocked"), 1)]
      end
    end

    describe "markbusy" do
      it "marks shards busy" do
        ns.create_shard info("localhost", "t_0_000", "TestShard")

        gizzmo "markbusy localhost/t_0_000"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard", "", "", 1)]
      end
    end

    describe "markunbusy" do
      it "marks shards as not busy" do
        ns.create_shard info("localhost", "t_0_000", "TestShard")
        gizzmo "markbusy localhost/t_0_000"

        gizzmo "markunbusy localhost/t_0_000"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard", "", "", 0)]
      end
    end

    describe "addforwarding" do
      it "adds a forwarding" do
        ns.create_shard info("localhost", "t_0_000", "TestShard")

        gizzmo "addforwarding 0 0 localhost/t_0_000"

        nameserver_db[:shards].should      == [info("localhost", "t_0_000", "TestShard")]
        nameserver_db[:forwardings].should == [forwarding(0, 0, id("localhost", "t_0_000"))]
      end
    end

    describe "deleteforwarding" do
      it "removes a forwarding" do
        ns.create_shard info("localhost", "t_0_000", "TestShard")

        gizzmo "addforwarding 0 0 localhost/t_0_000"
        gizzmo "deleteforwarding 0 0 localhost/t_0_000"

        nameserver_db[:shards].should      == [info("localhost", "t_0_000", "TestShard")]
        nameserver_db[:forwardings].should == []
      end
    end

    describe "addlink" do
      it "links two shards" do
        ns.create_shard info("localhost", "t_0_000", "TestShard")
        ns.create_shard info("localhost", "t_0_000_replicating", "ReplicatingShard")

        gizzmo "addlink localhost/t_0_000_replicating localhost/t_0_000 1"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard"),
                                          info("localhost", "t_0_000_replicating", "ReplicatingShard")]

        nameserver_db[:links].should == [link(id("localhost", "t_0_000_replicating"), id("localhost", "t_0_000"), 1)]
      end
    end

    describe "unlink" do
      it "unlinks two shards" do
        ns.create_shard info("localhost", "t_0_000", "TestShard")
        ns.create_shard info("localhost", "t_0_000_replicating", "ReplicatingShard")

        gizzmo "addlink localhost/t_0_000_replicating localhost/t_0_000 1"
        gizzmo "unlink localhost/t_0_000_replicating localhost/t_0_000"

        nameserver_db[:shards].should == [info("localhost", "t_0_000", "TestShard"),
                                          info("localhost", "t_0_000_replicating", "ReplicatingShard")]

        nameserver_db[:links].should == []
      end
    end


    describe "add-host" do
      it "creates single and multiple hosts" do
        gizzmo "add-host c1:c1host1:7777"
        gizzmo "add-host c2:c2host1:7777 c2:c2host2:7777"

        nameserver_db[:hosts].should == [host("c1host1", 7777, "c1"),
                                         host("c2host1", 7777, "c2"),
                                         host("c2host2", 7777, "c2")]
      end
    end

    describe "remove-host" do
      it "creates single and multiple hosts" do
        gizzmo "add-host c1:c1host1:7777"
        gizzmo "remove-host c1:c1host1:7777"

        nameserver_db[:hosts].should == []
      end
    end
  end

  describe "basic read methods" do
    before do
      3.times do |i|
        ns.create_shard info("localhost", "t_0_00#{i}_a", "TestShard", "Int", "Int")
        ns.create_shard info("127.0.0.1", "t_0_00#{i}_b", "TestShard", "Int", "Int")
        ns.create_shard info("localhost", "t_0_00#{i}_replicating", "ReplicatingShard")
        ns.add_link id("localhost", "t_0_00#{i}_replicating"), id("localhost", "t_0_00#{i}_a"), 1
        ns.add_link id("localhost", "t_0_00#{i}_replicating"), id("127.0.0.1", "t_0_00#{i}_b"), 1
        ns.set_forwarding forwarding(0, i, id("localhost", "t_0_00#{i}_replicating"))
      end
    end

    describe "subtree" do
      it "prints the tree for a shard" do
        results = "localhost/t_0_000_replicating\n  127.0.0.1/t_0_000_b\n  localhost/t_0_000_a\n"
        gizzmo("subtree localhost/t_0_000_replicating").should == results
        gizzmo("subtree localhost/t_0_000_a").should == results
        gizzmo("subtree 127.0.0.1/t_0_000_b").should == results
      end
    end

    describe "hosts" do
      it "prints a list of unique hosts" do
        gizzmo("hosts").should == "127.0.0.1\nlocalhost\n"
      end
    end

    describe "tables" do
      it "prints a list of table ids in the cluster" do
        gizzmo("tables").should == "0\n"
      end
    end

    describe "forwardings" do
      it "lists forwardings and the root of the corresponding shard trees" do
        gizzmo("forwardings").should == <<-EOF
0\t0\tlocalhost/t_0_000_replicating
0\t1\tlocalhost/t_0_001_replicating
0\t2\tlocalhost/t_0_002_replicating
        EOF
      end
    end

    describe "links" do
      it "lists links associated withe the given shards" do
        gizzmo("links localhost/t_0_000_a localhost/t_0_001_a").should == <<-EOF
localhost/t_0_000_replicating\tlocalhost/t_0_000_a\t1
localhost/t_0_001_replicating\tlocalhost/t_0_001_a\t1
        EOF
      end
    end

    describe "info" do
      it "outputs shard info for the given shard ids" do
        gizzmo("info localhost/t_0_000_a 127.0.0.1/t_0_001_b localhost/t_0_002_replicating").should == <<-EOF
localhost/t_0_000_a\tTestShard\tok
127.0.0.1/t_0_001_b\tTestShard\tok
localhost/t_0_002_replicating\tReplicatingShard\tok
        EOF
      end
    end

    describe "busy" do
      it "lists all busy shards" do
        gizzmo "markbusy localhost/t_0_000_a localhost/t_0_001_a localhost/t_0_002_a"

        gizzmo("busy").should == <<-EOF
localhost/t_0_000_a\tTestShard\tbusy
localhost/t_0_001_a\tTestShard\tbusy
localhost/t_0_002_a\tTestShard\tbusy
        EOF
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

    describe "topology" do
      it "lists counts for each template" do
        gizzmo("-T 0 topology").should == <<-EOF
   3 ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
        EOF
      end

      it "shows the template for each forwarding" do
        gizzmo("-T 0 topology --forwardings").should == <<-EOF
[0] 0 = localhost/t_0_000_replicating	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
[0] 1 = localhost/t_0_001_replicating	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
[0] 2 = localhost/t_0_002_replicating	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
        EOF
      end

      it "shows the template for each root shard" do
        gizzmo("-T 0 topology --shards").should == <<-EOF
localhost/t_0_000_replicating	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
localhost/t_0_001_replicating	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
localhost/t_0_002_replicating	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
        EOF
      end
    end
  end

  describe "find" do
    it "works"
  end

  describe "reload" do
    it "works"
  end

  describe "drill" do
    it "works"
  end

  describe "pair" do
    it "works"
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

  describe "repair-shards" do
    it "works"
  end

  describe "diff-shards" do
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

  describe "transform-tree" do
    it "works" do
      logname = 'test_log_name'
      ns.create_shard info("localhost", "s_0_001_a", "TestShard", "Int", "Int")
      ns.create_shard info("localhost", "s_0_001_replicating", "ReplicatingShard")
      ns.add_link id("localhost", "s_0_001_replicating"), id("localhost", "s_0_001_a"), 1
      ns.set_forwarding forwarding(0, 1, id("localhost", "s_0_001_replicating"))
      ns.reload_config

      gizzmo('-f transform-tree --no-progress --poll-interval=1 --rollback-log="%s" \
"ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1))" \
localhost/s_0_001_replicating' % logname).should == <<-EOF
ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1)) :
  PREPARE
    create_shard(BlockedShard)
    create_shard(TestShard/127.0.0.1)
    add_link(ReplicatingShard -> BlockedShard)
    add_link(BlockedShard -> TestShard/127.0.0.1)
  COPY
    copy_shard(TestShard/localhost -> TestShard/127.0.0.1)
  CLEANUP
    add_link(ReplicatingShard -> TestShard/127.0.0.1)
    commit_begin(ReplicatingShard)
    remove_link(ReplicatingShard -> BlockedShard)
    remove_link(BlockedShard -> TestShard/127.0.0.1)
    delete_shard(BlockedShard)
    commit_end(ReplicatingShard)
Applied to 1 shards

STARTING:
  [0] 1 = localhost/s_0_001_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
COPIES:
  localhost/s_0_001_a <-> 127.0.0.1/s_0_0001
FINISHING:
  [0] 1 = localhost/s_0_001_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
1 transformation applied. Total time elapsed: 1 second
      EOF

      # access the nameserver db directly for shard information
      nameserver_db[:shards].should == [info("127.0.0.1", "s_0_0001", "TestShard"),
                                        info("localhost", "s_0_001_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_0_001_replicating", "ReplicatingShard")]

      nameserver_db[:links].should == [link(id("localhost", "s_0_001_replicating"), id("127.0.0.1", "s_0_0001"), 1),
                                       link(id("localhost", "s_0_001_replicating"), id("localhost", "s_0_001_a"), 1)]

      # confirm that the last entry was recorded in a log of the correct name
      lastentry = ns.command_log(logname, false).peek(1)[0]
      Marshal.load(lastentry.binary_content).class.should == Gizzard::Transformation::Op::CommitEnd
    end
  end

  describe "transform" do
    it "works" do
      1.upto(2) do |i|
        ns.create_shard info("localhost", "s_0_00#{i}_a", "TestShard", "Int", "Int")
        ns.create_shard info("localhost", "s_0_00#{i}_replicating", "ReplicatingShard")
        ns.add_link id("localhost", "s_0_00#{i}_replicating"), id("localhost", "s_0_00#{i}_a"), 1
        ns.set_forwarding forwarding(0, i, id("localhost", "s_0_00#{i}_replicating"))
      end
      ns.reload_config

      gizzmo('-f -T0 transform --no-progress --poll-interval=1 \
"ReplicatingShard -> TestShard(localhost,1,Int,Int)" \
"ReplicatingShard -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1))"').should match(Regexp.new(Regexp.escape(<<-EOF).gsub("X", "\\d").gsub("second", "seconds?")))
ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1)) :
  PREPARE
    create_shard(BlockedShard)
    create_shard(TestShard/127.0.0.1)
    add_link(ReplicatingShard -> BlockedShard)
    add_link(BlockedShard -> TestShard/127.0.0.1)
  COPY
    copy_shard(TestShard/localhost -> TestShard/127.0.0.1)
  CLEANUP
    add_link(ReplicatingShard -> TestShard/127.0.0.1)
    commit_begin(ReplicatingShard)
    remove_link(ReplicatingShard -> BlockedShard)
    remove_link(BlockedShard -> TestShard/127.0.0.1)
    delete_shard(BlockedShard)
    commit_end(ReplicatingShard)
Applied to 2 shards

STARTING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
COPIES:
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
FINISHING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
2 transformations applied. Total time elapsed: X second
      EOF

      nameserver_db[:shards].should == [info("127.0.0.1", "s_0_0001", "TestShard"),
                                        info("127.0.0.1", "s_0_0002", "TestShard"),
                                        info("localhost", "s_0_001_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_0_001_replicating", "ReplicatingShard"),
                                        info("localhost", "s_0_002_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_0_002_replicating", "ReplicatingShard")]

      nameserver_db[:links].should == [link(id("localhost", "s_0_001_replicating"), id("127.0.0.1", "s_0_0001"), 1),
                                       link(id("localhost", "s_0_001_replicating"), id("localhost", "s_0_001_a"), 1),
                                       link(id("localhost", "s_0_002_replicating"), id("127.0.0.1", "s_0_0002"), 1),
                                       link(id("localhost", "s_0_002_replicating"), id("localhost", "s_0_002_a"), 1)]
    end

    it "properly re-weights shards" do
      1.upto(2) do |i|
        gizzmo "create TestShard -s Int -d Int localhost/s_0_00#{i}_a"
        gizzmo "create ReplicatingShard localhost/s_0_00#{i}_replicating"
        gizzmo "addlink localhost/s_0_00#{i}_replicating localhost/s_0_00#{i}_a 1"
        gizzmo "addforwarding 0 #{i} localhost/s_0_00#{i}_replicating"
      end
      gizzmo "-f reload"

      gizzmo('-f -T0 transform --no-progress --poll-interval=1 --max-copies=1 \
"ReplicatingShard -> TestShard(localhost,1,Int,Int)" \
"ReplicatingShard -> TestShard(localhost,3,Int,Int)"').should match(Regexp.new(Regexp.escape(<<-EOF).gsub("X", "\\d").gsub("second", "seconds?")))
ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> TestShard(localhost,3,Int,Int) :
  PREPARE
    add_link(ReplicatingShard -> TestShard/localhost)
    commit_begin(ReplicatingShard)
    remove_link(ReplicatingShard -> TestShard/localhost)
    commit_end(ReplicatingShard)
Applied to 2 shards

STARTING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> TestShard(localhost,3,Int,Int)
FINISHING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> TestShard(localhost,3,Int,Int)
STARTING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> TestShard(localhost,3,Int,Int)
FINISHING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> TestShard(localhost,3,Int,Int)
2 transformations applied. Total time elapsed: X second
EOF
    end

    it "works with multiple pages" do
      1.upto(2) do |i|
        gizzmo "create TestShard -s Int -d Int localhost/s_0_00#{i}_a"
        gizzmo "create ReplicatingShard localhost/s_0_00#{i}_replicating"
        gizzmo "addlink localhost/s_0_00#{i}_replicating localhost/s_0_00#{i}_a 1"
        gizzmo "addforwarding 0 #{i} localhost/s_0_00#{i}_replicating"
      end
      gizzmo "-f reload"

      gizzmo('-f -T0 transform --no-progress --poll-interval=1 --max-copies=1 \
"ReplicatingShard -> TestShard(localhost,1,Int,Int)" \
"ReplicatingShard -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1))"').should match(Regexp.new(Regexp.escape(<<-EOF).gsub("X", "\\d").gsub("second", "seconds?")))
ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1)) :
  PREPARE
    create_shard(BlockedShard)
    create_shard(TestShard/127.0.0.1)
    add_link(ReplicatingShard -> BlockedShard)
    add_link(BlockedShard -> TestShard/127.0.0.1)
  COPY
    copy_shard(TestShard/localhost -> TestShard/127.0.0.1)
  CLEANUP
    add_link(ReplicatingShard -> TestShard/127.0.0.1)
    commit_begin(ReplicatingShard)
    remove_link(ReplicatingShard -> BlockedShard)
    remove_link(BlockedShard -> TestShard/127.0.0.1)
    delete_shard(BlockedShard)
    commit_end(ReplicatingShard)
Applied to 2 shards

STARTING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
COPIES:
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
FINISHING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
STARTING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
COPIES:
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
FINISHING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
2 transformations applied. Total time elapsed: X second
      EOF

      nameserver_db[:shards].should == [info("127.0.0.1", "s_0_0001", "TestShard"),
                                        info("127.0.0.1", "s_0_0002", "TestShard"),
                                        info("localhost", "s_0_001_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_0_001_replicating", "ReplicatingShard"),
                                        info("localhost", "s_0_002_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_0_002_replicating", "ReplicatingShard")]

      nameserver_db[:links].should == [link(id("localhost", "s_0_001_replicating"), id("127.0.0.1", "s_0_0001"), 1),
                                       link(id("localhost", "s_0_001_replicating"), id("localhost", "s_0_001_a"), 1),
                                       link(id("localhost", "s_0_002_replicating"), id("127.0.0.1", "s_0_0002"), 1),
                                       link(id("localhost", "s_0_002_replicating"), id("localhost", "s_0_002_a"), 1)]
    end

    it "works with multiple forwarding tables" do
      0.upto(1) do |table|
        1.upto(2) do |i|
          ns.create_shard info("localhost", "s_#{table}_00#{i}_a", "TestShard", "Int", "Int")
          ns.create_shard info("localhost", "s_#{table}_00#{i}_replicating", "ReplicatingShard")
          ns.add_link id("localhost", "s_#{table}_00#{i}_replicating"), id("localhost", "s_#{table}_00#{i}_a"), 1
          ns.set_forwarding forwarding(table, i, id("localhost", "s_#{table}_00#{i}_replicating"))
        end
      end
      ns.reload_config

      gizzmo('-f -T0,1 transform --no-progress --poll-interval=1 \
"ReplicatingShard -> TestShard(localhost,1,Int,Int)" \
"ReplicatingShard -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1))"').should match(Regexp.new(Regexp.escape(<<-EOF).gsub("X", "\\d").gsub("second", "seconds?")))
ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1)) :
  PREPARE
    create_shard(BlockedShard)
    create_shard(TestShard/127.0.0.1)
    add_link(ReplicatingShard -> BlockedShard)
    add_link(BlockedShard -> TestShard/127.0.0.1)
  COPY
    copy_shard(TestShard/localhost -> TestShard/127.0.0.1)
  CLEANUP
    add_link(ReplicatingShard -> TestShard/127.0.0.1)
    commit_begin(ReplicatingShard)
    remove_link(ReplicatingShard -> BlockedShard)
    remove_link(BlockedShard -> TestShard/127.0.0.1)
    delete_shard(BlockedShard)
    commit_end(ReplicatingShard)
Applied to 4 shards

STARTING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [1] X = localhost/s_1_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [1] X = localhost/s_1_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
COPIES:
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
  localhost/s_1_00X_a <-> 127.0.0.1/s_1_000X
  localhost/s_1_00X_a <-> 127.0.0.1/s_1_000X
FINISHING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [1] X = localhost/s_1_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
  [1] X = localhost/s_1_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1))
4 transformations applied. Total time elapsed: X second
      EOF

      nameserver_db[:shards].should == [info("127.0.0.1", "s_0_0001", "TestShard"),
                                        info("127.0.0.1", "s_0_0002", "TestShard"),
                                        info("127.0.0.1", "s_1_0001", "TestShard"),
                                        info("127.0.0.1", "s_1_0002", "TestShard"),
                                        info("localhost", "s_0_001_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_0_001_replicating", "ReplicatingShard"),
                                        info("localhost", "s_0_002_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_0_002_replicating", "ReplicatingShard"),
                                        info("localhost", "s_1_001_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_1_001_replicating", "ReplicatingShard"),
                                        info("localhost", "s_1_002_a", "TestShard", "Int", "Int"),
                                        info("localhost", "s_1_002_replicating", "ReplicatingShard")]

      nameserver_db[:links].should == [link(id("localhost", "s_0_001_replicating"), id("127.0.0.1", "s_0_0001"), 1),
                                       link(id("localhost", "s_0_001_replicating"), id("localhost", "s_0_001_a"), 1),
                                       link(id("localhost", "s_0_002_replicating"), id("127.0.0.1", "s_0_0002"), 1),
                                       link(id("localhost", "s_0_002_replicating"), id("localhost", "s_0_002_a"), 1),
                                       link(id("localhost", "s_1_001_replicating"), id("127.0.0.1", "s_1_0001"), 1),
                                       link(id("localhost", "s_1_001_replicating"), id("localhost", "s_1_001_a"), 1),
                                       link(id("localhost", "s_1_002_replicating"), id("127.0.0.1", "s_1_0002"), 1),
                                       link(id("localhost", "s_1_002_replicating"), id("localhost", "s_1_002_a"), 1)]
    end
  end

  describe "rebalance" do
    it "works" do
      1.upto(8) do |i|
        ns.create_shard info("localhost","s_0_00#{i}_a","TestShard")
        ns.create_shard info("localhost","s_0_00#{i}_replicating","ReplicatingShard")
        ns.add_link id("localhost", "s_0_00#{i}_replicating"), id("localhost", "s_0_00#{i}_a"), 1
        ns.set_forwarding forwarding(0,i,id("localhost", "s_0_00#{i}_replicating"))
      end
      ns.reload_config
      gizzmo('-f -T0 rebalance --no-progress --poll-interval=1 \
1 "ReplicatingShard -> TestShard(127.0.0.1,1)" \
1 "ReplicatingShard -> TestShard(localhost,1)"').should match(Regexp.new(Regexp.escape(<<-EOF).gsub("X", "\\d").gsub("second", "seconds?")))
ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1) :
  PREPARE
    create_shard(BlockedShard)
    create_shard(TestShard/127.0.0.1)
    add_link(ReplicatingShard -> BlockedShard)
    add_link(BlockedShard -> TestShard/127.0.0.1)
  COPY
    copy_shard(TestShard/localhost -> TestShard/127.0.0.1)
  CLEANUP
    add_link(ReplicatingShard -> TestShard/127.0.0.1)
    commit_begin(ReplicatingShard)
    remove_link(ReplicatingShard -> BlockedShard)
    remove_link(BlockedShard -> TestShard/127.0.0.1)
    remove_link(ReplicatingShard -> TestShard/localhost)
    delete_shard(BlockedShard)
    delete_shard(TestShard/localhost)
    commit_end(ReplicatingShard)
Applied to 4 shards

STARTING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
COPIES:
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
  localhost/s_0_00X_a <-> 127.0.0.1/s_0_000X
FINISHING:
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
  [0] X = localhost/s_0_00X_replicating: ReplicatingShard(1) -> TestShard(localhost,1) => ReplicatingShard(1) -> TestShard(127.0.0.1,1)
4 transformations applied. Total time elapsed: X second
      EOF
    end
  end

  describe "create-table" do
    it "works" do
      gizzmo('-f -T0,1 create-table --shards=4 --base-name=s \
1 "ReplicatingShard -> TestShard(127.0.0.1,1)" \
1 "ReplicatingShard -> TestShard(localhost,1)"').should match(Regexp.new(Regexp.escape(<<-EOF).gsub("X", "\\d")))
Create tables 0, 1:
  ReplicatingShard(1) -> TestShard(127.0.0.1,1)
  for 2 base ids:
    288230376151711744
    576460752303423488
  ReplicatingShard(1) -> TestShard(localhost,1)
  for 2 base ids:
    864691128455135232
    0

create_shard(TestShard/127.0.0.1): s_0_0003
create_shard(ReplicatingShard): s_0_0003
add_link(ReplicatingShard -> TestShard/127.0.0.1): s_0_0003
set_forwarding(ReplicatingShard): s_0_0003
create_shard(TestShard/127.0.0.1): s_0_0002
create_shard(ReplicatingShard): s_0_0002
add_link(ReplicatingShard -> TestShard/127.0.0.1): s_0_0002
set_forwarding(ReplicatingShard): s_0_0002
create_shard(ReplicatingShard): s_0_0001
create_shard(TestShard/localhost): s_0_0001
add_link(ReplicatingShard -> TestShard/localhost): s_0_0001
set_forwarding(ReplicatingShard): s_0_0001
create_shard(ReplicatingShard): s_0_0000
create_shard(TestShard/localhost): s_0_0000
add_link(ReplicatingShard -> TestShard/localhost): s_0_0000
set_forwarding(ReplicatingShard): s_0_0000
create_shard(TestShard/127.0.0.1): s_1_0003
create_shard(ReplicatingShard): s_1_0003
add_link(ReplicatingShard -> TestShard/127.0.0.1): s_1_0003
set_forwarding(ReplicatingShard): s_1_0003
create_shard(TestShard/127.0.0.1): s_1_0002
create_shard(ReplicatingShard): s_1_0002
add_link(ReplicatingShard -> TestShard/127.0.0.1): s_1_0002
set_forwarding(ReplicatingShard): s_1_0002
create_shard(ReplicatingShard): s_1_0001
create_shard(TestShard/localhost): s_1_0001
add_link(ReplicatingShard -> TestShard/localhost): s_1_0001
set_forwarding(ReplicatingShard): s_1_0001
create_shard(ReplicatingShard): s_1_0000
create_shard(TestShard/localhost): s_1_0000
add_link(ReplicatingShard -> TestShard/localhost): s_1_0000
set_forwarding(ReplicatingShard): s_1_0000
      EOF

      nameserver_db[:shards].should == [info("127.0.0.1", "s_0_0002", "TestShard"),
                                        info("127.0.0.1", "s_0_0003", "TestShard"),
                                        info("127.0.0.1", "s_1_0002", "TestShard"),
                                        info("127.0.0.1", "s_1_0003", "TestShard"),
                                        info("localhost", "s_0_0000", "TestShard"),
                                        info("localhost", "s_0_0000_replicating", "ReplicatingShard"),
                                        info("localhost", "s_0_0001", "TestShard"),
                                        info("localhost", "s_0_0001_replicating", "ReplicatingShard"),
                                        info("localhost", "s_0_0002_replicating", "ReplicatingShard"),
                                        info("localhost", "s_0_0003_replicating", "ReplicatingShard"),
                                        info("localhost", "s_1_0000", "TestShard"),
                                        info("localhost", "s_1_0000_replicating", "ReplicatingShard"),
                                        info("localhost", "s_1_0001", "TestShard"),
                                        info("localhost", "s_1_0001_replicating", "ReplicatingShard"),
                                        info("localhost", "s_1_0002_replicating", "ReplicatingShard"),
                                        info("localhost", "s_1_0003_replicating", "ReplicatingShard")]

      nameserver_db[:links].should == [link(id("localhost", "s_0_0000_replicating"), id("localhost", "s_0_0000"), 1),
                                       link(id("localhost", "s_0_0001_replicating"), id("localhost", "s_0_0001"), 1),
                                       link(id("localhost", "s_0_0002_replicating"), id("127.0.0.1", "s_0_0002"), 1),
                                       link(id("localhost", "s_0_0003_replicating"), id("127.0.0.1", "s_0_0003"), 1),
                                       link(id("localhost", "s_1_0000_replicating"), id("localhost", "s_1_0000"), 1),
                                       link(id("localhost", "s_1_0001_replicating"), id("localhost", "s_1_0001"), 1),
                                       link(id("localhost", "s_1_0002_replicating"), id("127.0.0.1", "s_1_0002"), 1),
                                       link(id("localhost", "s_1_0003_replicating"), id("127.0.0.1", "s_1_0003"), 1)]

      nameserver_db[:forwardings].should == [forwarding(0, 0, id("localhost", "s_0_0000_replicating")),
                                             forwarding(1, 0, id("localhost", "s_1_0000_replicating")),
                                             forwarding(0, 288230376151711744, id("localhost", "s_0_0003_replicating")),
                                             forwarding(1, 288230376151711744, id("localhost", "s_1_0003_replicating")),
                                             forwarding(0, 576460752303423488, id("localhost", "s_0_0002_replicating")),
                                             forwarding(1, 576460752303423488, id("localhost", "s_1_0002_replicating")),
                                             forwarding(0, 864691128455135232, id("localhost", "s_0_0001_replicating")),
                                             forwarding(1, 864691128455135232, id("localhost", "s_1_0001_replicating"))]

    end
  end
end
