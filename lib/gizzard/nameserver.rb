module Gizzard
  class Shard < Struct.new(:info, :children, :weight)
    class << self
      def canonical_table_prefix(enum, table_id = nil, base_prefix = "shard")
        enum_s         = "%0.4i" % enum
        table_id_s     = table_id.nil? ? nil : table_id < 0 ? "n#{table_id.abs}" : table_id.to_s
        [base_prefix, table_id_s, enum_s].compact.join('_')
      end

      def parse_enumeration(table_prefix)
        if match = table_prefix.match(/\d{3,}/)
          match[0].to_i
        else
          raise "Cannot derive enumeration!"
        end
      end
    end

    VIRTUAL_SHARD_TYPES = [
      "FailingOverShard",
      "ReplicatingShard",
      "ReadOnlyShard",
      "WriteOnlyShard",
      "BlockedShard",
    ]

    REPLICATING_SHARD_TYPES = ["ReplicatingShard", "FailingOverShard"]

    TRANSITIONAL_SHARD_TYPES = ["BlockedShard"]

    INVALID_COPY_TYPES = ["ReadOnlyShard", "BlackHoleShard", "BlockedShard", "WriteOnlyShard"]

    SHARD_SUFFIXES = {
      "FailingOverShard" => 'replicating',
      "ReplicatingShard" => 'replicating',
      "ReadOnlyShard" => 'read_only',
      "WriteOnlyShard" => 'write_only',
      "BlockedShard" => 'blocked'
    }

    SHARD_TAGS = {
      "ReplicatingShard" => 'replicating',
      "ReadOnlyShard" => 'read_only',
      "WriteOnlyShard" => 'write_only',
      "BlockedShard" => 'blocked',
      "BlackHoleShard" => 'blackhole'
    }

    def id; info.id end
    def hostname; id.hostname end
    def table_prefix; id.table_prefix end
    def class_name; info.class_name end
    def source_type; info.source_type end
    def destination_type; info.destination_type end
    def busy; info.busy end

    def template
      child_templates = children.map {|c| c.template }

      ShardTemplate.new(info.class_name,
                        id.hostname,
                        weight,
                        info.source_type,
                        info.destination_type,
                        child_templates)
    end

    def enumeration
      self.class.parse_enumeration(table_prefix)
    end

    def canonical_shard_id_map(base_prefix = "shard", table_id = nil, enum = nil)
      enum         ||= self.enumeration
      base           = Shard.canonical_table_prefix(enum, table_id, base_prefix)
      suffix         = SHARD_SUFFIXES[class_name.split('.').last]
      canonical_name = [base, suffix].compact.join('_')
      canonical_id   = ShardId.new(self.hostname, canonical_name)

      children.inject(canonical_id => self.id) do |m, c|
        m.update c.canonical_shard_id_map(base_prefix, table_id, enum)
      end
    end
  end

  class Nameserver
    DEFAULT_PORT    = 7920
    DEFAULT_RETRIES = 10
    MAX_ATTEMPT_SECS = 10
    PARALLELISM     = 10

    attr_reader :hosts, :logfile, :dryrun, :framed
    alias dryrun? dryrun

    def initialize(*hosts)
      options = hosts.last.is_a?(Hash) ? hosts.pop : {}
      @retries = options[:retries] || DEFAULT_RETRIES
      @logfile = options[:log]     || "/tmp/gizzmo.log"
      @dryrun  = options[:dry_run] || false
      @framed  = options[:framed]  || false
      @hosts   = hosts.flatten
    end

    def get_shards(ids)
      ids.map {|id| with_retry { random_client.get_shard(id) } }
    end

    def reload_updated_forwardings
      on_all_servers "reload_updated_forwardings" do |c|
        with_retry { c.reload_updated_forwardings }
      end
    end

    def reload_config
      on_all_servers "reload_config" do |c|
        with_retry { c.reload_config }
      end
    end

    def copy_shard(*shards)
      with_retry { random_client.copy_shard(*shards) }
    end

    def repair_shards(*shards)
      with_retry { random_client.repair_shard(*shards) }
    end

    def diff_shards(*shards)
      with_retry { random_client.diff_shards(*shards) }
    end

    def respond_to?(method)
      client.respond_to? method or super
    end

    def method_missing(method, *args, &block)
      if client.respond_to?(method)
        with_retry { random_client.send(method, *args, &block) }
      else
        super
      end
    end

    def manifest(*table_ids)
      Manifest.new(self, table_ids)
    end

    # confirm that all clients are connected to the same cluster
    def validate_clients_or_raise
      last_client_host = nil
      last_client_hostset = nil
      # linear equality comparison for the host sets of each appserver
      all_clients.map do |client|
        this_client_hostset = client.list_hostnames.inject({}) do |hostnames, hostname|
          hostnames[hostname] = true
          hostnames
        end
        if last_client_hostset != nil && this_client_hostset != last_client_hostset
          raise "App-servers #{last_client_host} and #{client.get_host} disagree on the set" +
              " of shard hosts: #{last_client_hostset.keys} vs #{this_client_hostset.keys}"
        end
        last_client_host = client.get_host
        last_client_hostset = this_client_hostset
      end
    end

    private

    # executes the given block in parallel with a client for each server: in the face of failure,
    # may return less results than there are clients
    def on_all_servers(operation_name, &block)
      # fork into many threads, and then join with exception handling
      clients_and_threads = all_clients.map do |client|
        [client, Thread.new { Thread.current[:result] = block.call(client) }]
      end
      clients_and_results_or_exceptions = clients_and_threads.map do |client, thread|
        begin
          thread.join
          [client, thread[:result], nil]
        rescue Exception => e
          [client, nil, e]
        end
      end

      successful_clients, failed_clients =
        clients_and_results_or_exceptions.partition{|_, _, exception| exception.nil? }
      if failed_clients.size > 0
        # if there were failed clients, but the user would like to proceed anyway,
        # mutate @all_clients to remove the failed clients
        puts "#{failed_clients.size} of #{all_clients.size} clients failed to execute '#{operation_name}':"
        failed_clients.each do |client, _, exception|
          puts "\t#{client.get_host} failed with: #{exception}"
        end
        # TODO: propagate 'force' parameter here, and kill-if-force
        Gizzard::confirm!(false, "Proceed without these hosts?")
        # we're still alive: user wanted to proceed
        @all_clients.reject!(failed_clients.map{|client, _, _| client })
      end

      # return only successful results
      successful_clients.map{|_, result, _| result }
    end

    def client
      @client ||= create_client(hosts.first)
    end

    def all_clients
      @all_clients ||= hosts.map {|host| create_client(host) }
    end

    def random_client
      all_clients[rand(all_clients.length)]
    end

    def create_client(host)
      host, port = host.split(":")
      port ||= DEFAULT_PORT
      Manager.new(host, port.to_i, logfile, framed, dryrun)
    end

    private

    def with_retry
      times ||= @retries
      yield
    rescue Exception => e
      STDERR.puts "\nException: #{e.class}: #{e.description rescue "(no description)"}"
      STDERR.puts "Retrying #{times} more time#{'s' if times > 1}..." if times > 0
      times -= 1
      sleep_time = MAX_ATTEMPT_SECS / [times, 1].max
      (times < 0) ? raise : (sleep(sleep_time); retry)
    end

    class Manifest
      attr_reader :forwardings, :links, :shard_infos, :trees, :templates

      def initialize(nameserver, table_ids)
        states = nameserver.dump_nameserver(table_ids)

        @forwardings = states.map {|s| s.forwardings }.flatten

        @links = states.map {|s| s.links }.flatten.inject({}) do |h, link|
          (h[link.up_id] ||= []) << [link.down_id, link.weight]; h
        end

        @shard_infos = states.map {|s| s.shards }.flatten.inject({}) do |h, shard|
          h.update shard.id => shard
        end

        @trees = @forwardings.inject({}) do |h, forwarding|
          h.update forwarding => build_tree(forwarding.shard_id)
        end

        @templates = @trees.inject({}) do |h, (forwarding, shard)|
          (h[shard.template] ||= []) << forwarding; h
        end
      end

      # wraps pre-write validation around manager.manifest
      def validate_for_write_or_raise(ignore_busy, ignore_shard_types)
        blocked_types = Shard::TRANSITIONAL_SHARD_TYPES - ignore_shard_types
        return if ignore_busy && !blocked_types.empty?
        shard_infos.each do |shard_id, shard_info|
          if shard_info.busy? && !ignore_busy
            puts "Aborting due to busy shard #{shard_id.inspect}"
            exit 1
          end
          shard_type = shard_info.class_name.split('.').last
          if blocked_types.include? shard_type
            puts "Aborting due to #{shard_type} shard: #{shard_id.inspect}"
            exit 1
          end
        end
      end

      private

      def get_filtered_forwardings(nameserver, filter)
        return filter[:forwardings] if filter[:forwardings]

        forwardings = nameserver.get_forwardings

        if table_id = filter[:table_id]
          forwardings.reject! {|f| f.table_id != table_id }
        end

        forwardings
      end

      def build_tree(shard_id, link_weight=ShardTemplate::DEFAULT_WEIGHT)
        children = (links[shard_id] || []).map do |(child_id, child_weight)|
          build_tree(child_id, child_weight)
        end

        info = shard_infos[shard_id] or raise "shard info not found for: #{shard_id}"
        Shard.new(info, children, link_weight)
      end
    end
  end
end
