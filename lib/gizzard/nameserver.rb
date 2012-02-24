module Gizzard
  module ParallelMap
    def parallel_map(enumerable, &block)
      enumerable.map do |elem|
        Thread.new { Thread.current[:result] = block.call(elem) }
      end.map do |thread|
        thread.join
        thread[:result]
      end
    end
  end

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

    TRANSITIONAL_SHARD_TYPES = ["BlackHoleShard", "BlockedShard"]

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
    include ParallelMap

    DEFAULT_PORT    = 7920
    DEFAULT_RETRIES = 10
    PARALLELISM     = 10

    attr_reader :hosts, :logfile, :dryrun, :framed
    alias dryrun? dryrun

    def initialize(*hosts)
      # TODO: waaaaaat
      options = hosts.last.is_a?(Hash) ? hosts.pop : {}
      @retries = options[:retries] || DEFAULT_RETRIES
      @logfile = options[:log]     || "/tmp/gizzmo.log"
      @dryrun  = options[:dry_run] || false
      @framed  = options[:framed]  || false
      @hosts   = hosts.flatten
    end

    def get_shards(ids)
      ids.map {|id| with_retry { client.get_shard(id) } }
    end

    def reload_updated_forwardings
      parallel_map all_clients do |c|
        with_retry { c.reload_updated_forwardings }
      end
    end

    def reload_config
      parallel_map all_clients do |c|
        with_retry { c.reload_config }
      end
    end

    def copy_shard(*shards)
      c = random_client
      with_retry { c.copy_shard(*shards) }
    end

    def repair_shards(*shards)
      c = random_client
      with_retry { c.repair_shard(*shards) }
    end

    def diff_shards(*shards)
      c = random_client
      with_retry { c.diff_shards(*shards) }
    end

    def respond_to?(method)
      client.respond_to? method or super
    end

    def method_missing(method, *args, &block)
      if client.respond_to?(method)
        with_retry { client.send(method, *args, &block) }
      else
        super
      end
    end

    def manifest(*table_ids)
      Manifest.new(self, table_ids)
    end

    def command_log(name, create)
      CommandLog.new(self, name, create)
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
      (times < 0) ? raise : (sleep 0.1; retry)
    end

    class Manifest
      include ParallelMap

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
            puts "Aborting due to blocked shard #{shard_id.inspect}"
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

    class CommandLog
      def initialize(nameserver, log_name, create)
        @nameserver = nameserver
        @name = log_name
        @log_id =
          if create
            nameserver.log_create(log_name)
          else
            nameserver.log_get(log_name)
          end
      end

      # pushes binary content to the end of the log, returns a new log_entry_id
      def push!(binary_content)
        @nameserver.log_entry_push(@log_id, binary_content)
      end

      # returns the top LogEntry tuple for the log
      def peek
        @nameserver.log_entry_peek(@log_id)
      end

      # pops the given log_entry_id (which must be at the top of the log)
      def pop!(log_entry_id)
        @nameserver.log_entry_pop(@log_id, log_entry_id)
      end
    end
  end
end
