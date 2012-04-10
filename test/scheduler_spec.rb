require File.expand_path('../spec_helper', __FILE__)

describe Gizzard::Transformation::Scheduler do

  before do
    @nameserver = stub!.subject
    stub(@nameserver).dryrun? { false }

    @transformations = {}
    @scheduler = Gizzard::Transformation::Scheduler.new(@nameserver, @nameserver, 't', @transformations)
  end

  describe "busy_shards" do
    it "memoizes" do
      shards = [info('127.0.0.1', 't_0_0001', 'TestShard')]
      mock(@nameserver).get_busy_shards { shards }

      @scheduler.busy_shards.should == shards.map {|s| s.id }.to_set
      @scheduler.busy_shards.should == shards.map {|s| s.id }.to_set
    end

    it "resets after calling reload_busy_shards" do
      shards = [info('127.0.0.1', 't_0_0001', 'TestShard')]
      mock(@nameserver).get_busy_shards { shards }.twice

      @scheduler.busy_shards.should == shards.map {|s| s.id }.to_set
      @scheduler.reload_busy_shards
      @scheduler.busy_shards.should == shards.map {|s| s.id }.to_set
    end
  end

  describe "busy_hosts" do
    it "returns a list of hosts over the threshold of copies per host" do
      shards = []
      stub(@nameserver).get_busy_shards { shards }
      @scheduler = Gizzard::Transformation::Scheduler.new(@nameserver, @nameserver, 't', @transformations, :copies_per_host => 2)

      @scheduler.busy_hosts.should == Set.new

      shards = [info('127.0.0.1', 't_0_0001', 'TestShard')]
      @scheduler.reload_busy_shards
      @scheduler.busy_hosts.should == Set.new

      shards = [info('127.0.0.1', 't_0_0001', 'TestShard'), info('127.0.0.1', 't_0_0002', 'TestShard')]
      @scheduler.reload_busy_shards
      @scheduler.busy_hosts.should == ['127.0.0.1'].to_set
    end

    it "respects passed in extra hosts" do
      shards = []
      stub(@nameserver).get_busy_shards { shards }
      @scheduler = Gizzard::Transformation::Scheduler.new(@nameserver, @nameserver, 't', @transformations, :copies_per_host => 2)

      @scheduler.busy_hosts.should == Set.new
      @scheduler.busy_hosts(["127.0.0.1"]).should == Set.new
      @scheduler.busy_hosts(["127.0.0.1", "127.0.0.1"]).should == ["127.0.0.1"].to_set
    end
  end
end
