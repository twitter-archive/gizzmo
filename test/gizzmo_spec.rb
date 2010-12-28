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

  describe "basic manipulation commands" do
    describe "create" do
      it "creates a single shard" do
        gizzmo "create TestShard localhost/t0_0"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard")]
      end

      it "creates multiple shards" do
        gizzmo "create TestShard localhost/t0_0 localhost/t0_1"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard"),
                                       info("localhost", "t0_1", "TestShard")]
      end

      it "honors source and destination types" do
        gizzmo "create TestShard -s int -d long localhost/t0_0"
        gizzmo "create TestShard --source-type=int --destination-type=long localhost/t0_1"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard", "int", "long"),
                                       info("localhost", "t0_1", "TestShard", "int", "long")]
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
        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard"),
                                       info("localhost", "t0_0_blocked", "BlockedShard"),
                                       info("localhost", "t0_0_replicating", "ReplicatingShard")]

        nameserver[:links].should == [link(id("localhost", "t0_0_blocked"), id("localhost", "t0_0"), 1),
                                      link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0_blocked"), 1)]
      end

      it "unwrap unwraps a shard" do
        gizzmo "unwrap localhost/t0_0_blocked"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard"),
                                       info("localhost", "t0_0_replicating", "ReplicatingShard")]

        nameserver[:links].should == [link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0"), 1)]
      end

      it "unwrap doesn't unwrap a top level shard or a leaf" do
        gizzmo "unwrap localhost/t0_0"
        gizzmo "unwrap localhost/t0_0_replicating"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard"),
                                       info("localhost", "t0_0_blocked", "BlockedShard"),
                                       info("localhost", "t0_0_replicating", "ReplicatingShard")]

        nameserver[:links].should == [link(id("localhost", "t0_0_blocked"), id("localhost", "t0_0"), 1),
                                      link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0_blocked"), 1)]
      end
    end

    describe "markbusy" do
      it "marks shards busy" do
        gizzmo "create TestShard localhost/t0_0"
        gizzmo "markbusy localhost/t0_0"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard", "", "", 1)]
      end
    end

    describe "markunbusy" do
      it "marks shards as not busy" do
        gizzmo "create TestShard localhost/t0_0"
        gizzmo "markbusy localhost/t0_0"
        gizzmo "markunbusy localhost/t0_0"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard", "", "", 0)]
      end
    end

    describe "addforwarding" do
      it "adds a forwarding" do
        gizzmo "create TestShard localhost/t0_0"
        gizzmo "addforwarding 0 0 localhost/t0_0"

        nameserver[:shards].should      == [info("localhost", "t0_0", "TestShard")]
        nameserver[:forwardings].should == [forwarding(0, 0, id("localhost", "t0_0"))]
      end
    end

    describe "deleteforwarding" do
      it "removes a forwarding" do
        gizzmo "create TestShard localhost/t0_0"
        gizzmo "addforwarding 0 0 localhost/t0_0"
        gizzmo "deleteforwarding 0 0 localhost/t0_0"

        nameserver[:shards].should      == [info("localhost", "t0_0", "TestShard")]
        nameserver[:forwardings].should == []
      end
    end

    describe "addlink" do
      it "links two shards" do
        gizzmo "create TestShard localhost/t0_0"
        gizzmo "create ReplicatingShard localhost/t0_0_replicating"
        gizzmo "addlink localhost/t0_0_replicating localhost/t0_0 1"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard"),
                                       info("localhost", "t0_0_replicating", "ReplicatingShard")]

        nameserver[:links].should == [link(id("localhost", "t0_0_replicating"), id("localhost", "t0_0"), 1)]
      end
    end

    describe "unlink" do
      it "unlinks two shards" do
        gizzmo "create TestShard localhost/t0_0"
        gizzmo "create ReplicatingShard localhost/t0_0_replicating"
        gizzmo "addlink localhost/t0_0_replicating localhost/t0_0 1"
        gizzmo "unlink localhost/t0_0_replicating localhost/t0_0"

        nameserver[:shards].should == [info("localhost", "t0_0", "TestShard"),
                                       info("localhost", "t0_0_replicating", "ReplicatingShard")]

        nameserver[:links].should == []
      end
    end


    describe "add-host" do
      it "creates single and multiple hosts" do
        gizzmo "add-host c1:c1host1:7777"
        gizzmo "add-host c2:c2host1:7777 c2:c2host2:7777"

        nameserver[:hosts].should == [ host("c1host1", 7777, "c1"),
                                       host("c2host1", 7777, "c2"),
                                       host("c2host2", 7777, "c2") ]
      end
    end

    describe "remove-host" do
      it "creates single and multiple hosts" do
        gizzmo "add-host c1:c1host1:7777"
        gizzmo "remove-host c1:c1host1:7777"

        nameserver[:hosts].should == []
      end
    end
  end

  describe "basic read methods" do
    before do
      3.times do |i|
        gizzmo "create TestShard -s Int -d Int localhost/t0_#{i}_a 127.0.0.1/t0_#{i}_b"
        gizzmo "create ReplicatingShard localhost/t0_#{i}_replicating"
        gizzmo "addlink localhost/t0_#{i}_replicating localhost/t0_#{i}_a 1"
        gizzmo "addlink localhost/t0_#{i}_replicating 127.0.0.1/t0_#{i}_b 1"
        gizzmo "addforwarding 0 #{i} localhost/t0_#{i}_replicating"
      end
    end

    describe "subtree" do
      it "prints the tree for a shard" do
        results = "localhost/t0_0_replicating\n  127.0.0.1/t0_0_b\n  localhost/t0_0_a\n"
        gizzmo("subtree localhost/t0_0_replicating").should == results
        gizzmo("subtree localhost/t0_0_a").should == results
        gizzmo("subtree 127.0.0.1/t0_0_b").should == results
      end
    end

    describe "hosts" do
      it "prints a list of unique hosts" do
        gizzmo("hosts").should == "127.0.0.1\nlocalhost\n"
      end
    end

    describe "forwardings" do
      it "lists forwardings and the root of the corresponding shard trees" do
        gizzmo("forwardings").should == <<-EOF
0\t0\tlocalhost/t0_0_replicating
0\t1\tlocalhost/t0_1_replicating
0\t2\tlocalhost/t0_2_replicating
        EOF
      end
    end

    describe "links" do
      it "lists links associated withe the given shards" do
        gizzmo("links localhost/t0_0_a localhost/t0_1_a").should == <<-EOF
localhost/t0_0_replicating\tlocalhost/t0_0_a\t1
localhost/t0_1_replicating\tlocalhost/t0_1_a\t1
        EOF
      end
    end

    describe "info" do
      it "outputs shard info for the given shard ids" do
        gizzmo("info localhost/t0_0_a 127.0.0.1/t0_1_b localhost/t0_2_replicating").should == <<-EOF
localhost/t0_0_a\tTestShard\tok
127.0.0.1/t0_1_b\tTestShard\tok
localhost/t0_2_replicating\tReplicatingShard\tok
        EOF
      end
    end

    describe "busy" do
      it "lists all busy shards" do
        gizzmo "markbusy localhost/t0_0_a localhost/t0_1_a localhost/t0_2_a"

        gizzmo("busy").should == <<-EOF
localhost/t0_0_a\tTestShard\tbusy
localhost/t0_1_a\tTestShard\tbusy
localhost/t0_2_a\tTestShard\tbusy
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
        gizzmo("topology 0").should == <<-EOF
   3 ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
        EOF
      end

      it "shows the template for each forwarding" do
        gizzmo("topology --forwardings 0").should == <<-EOF
                        0	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
                        1	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
                        2	ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1,Int,Int))
        EOF
      end
    end
  end

  describe "find" do
    it "works"
  end

  describe "rebalance" do
    it "works"
  end

  describe "repair" do
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
      gizzmo "create -s Int -d Int TestShard localhost/s_0_001_a"
      #gizzmo "create TestShard 127.0.0.1/s_0_001_b"
      gizzmo "create ReplicatingShard localhost/s_0_001_replicating"
      gizzmo "addlink localhost/s_0_001_replicating localhost/s_0_001_a 1"
      #gizzmo "addlink localhost/s_0_001_replicating 127.0.0.1/s_0_001_b 1"
      gizzmo "addforwarding 0 1 localhost/s_0_001_replicating"
      gizzmo "-f reload"

      gizzmo('-f transform-tree "ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1))" localhost/s_0_001_replicating').should == <<-EOF
ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1)) :
  PREPARE
    create_shard(TestShard/127.0.0.1)
    create_shard(WriteOnlyShard)
    add_link(WriteOnlyShard -> TestShard/127.0.0.1)
    add_link(ReplicatingShard -> WriteOnlyShard)
  COPY
    copy_shard(TestShard/127.0.0.1)
  CLEANUP
    add_link(ReplicatingShard -> TestShard/127.0.0.1)
    remove_link(WriteOnlyShard -> TestShard/127.0.0.1)
    remove_link(ReplicatingShard -> WriteOnlyShard)
    delete_shard(WriteOnlyShard)
      EOF

      nameserver[:shards].should == [ info("127.0.0.1", "s_0_0001", "TestShard"),
                                      info("localhost", "s_0_001_a", "TestShard", "Int", "Int"),
                                      info("localhost", "s_0_001_replicating", "ReplicatingShard") ]

      nameserver[:links].should == [ link(id("localhost", "s_0_001_replicating"), id("127.0.0.1", "s_0_0001"), 1),
                                     link(id("localhost", "s_0_001_replicating"), id("localhost", "s_0_001_a"), 1) ]
    end
  end

  describe "transform" do
    it "works" do
      1.upto(2) do |i|
        gizzmo "create TestShard -s Int -d Int localhost/s_0_00#{i}_a"
        #gizzmo "create TestShard -s Int -d Int 127.0.0.1/s_0_000#{i}_b"
        gizzmo "create ReplicatingShard localhost/s_0_00#{i}_replicating"
        gizzmo "addlink localhost/s_0_00#{i}_replicating localhost/s_0_00#{i}_a 1"
        #gizzmo "addlink localhost/s_0_00#{i}_replicating 127.0.0.1/s_0_000#{i}_b 1"
        gizzmo "addforwarding 0 #{i} localhost/s_0_00#{i}_replicating"
      end
      gizzmo "-f reload"

      gizzmo('-f transform 0 "ReplicatingShard -> TestShard(localhost,1,Int,Int)" "ReplicatingShard -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1))"').should == <<-EOF
ReplicatingShard(1) -> TestShard(localhost,1,Int,Int) => ReplicatingShard(1) -> (TestShard(localhost,1,Int,Int), TestShard(127.0.0.1,1)) :
  PREPARE
    create_shard(TestShard/127.0.0.1)
    create_shard(WriteOnlyShard)
    add_link(WriteOnlyShard -> TestShard/127.0.0.1)
    add_link(ReplicatingShard -> WriteOnlyShard)
  COPY
    copy_shard(TestShard/127.0.0.1)
  CLEANUP
    add_link(ReplicatingShard -> TestShard/127.0.0.1)
    remove_link(WriteOnlyShard -> TestShard/127.0.0.1)
    remove_link(ReplicatingShard -> WriteOnlyShard)
    delete_shard(WriteOnlyShard)
Applied to:
  [0] 1 -> localhost/s_0_001_replicating
  [0] 2 -> localhost/s_0_002_replicating
      EOF

      nameserver[:shards].should == [ info("127.0.0.1", "s_0_0001", "TestShard"),
                                      info("127.0.0.1", "s_0_0002", "TestShard"),
                                      info("localhost", "s_0_001_a", "TestShard", "Int", "Int"),
                                      info("localhost", "s_0_001_replicating", "ReplicatingShard"),
                                      info("localhost", "s_0_002_a", "TestShard", "Int", "Int"),
                                      info("localhost", "s_0_002_replicating", "ReplicatingShard") ]

      nameserver[:links].should == [ link(id("localhost", "s_0_001_replicating"), id("127.0.0.1", "s_0_0001"), 1),
                                     link(id("localhost", "s_0_001_replicating"), id("localhost", "s_0_001_a"), 1),
                                     link(id("localhost", "s_0_002_replicating"), id("127.0.0.1", "s_0_0002"), 1),
                                     link(id("localhost", "s_0_002_replicating"), id("localhost", "s_0_002_a"), 1) ]
    end
  end
end
