module Gizzard
  class Nameserver

    DEFAULT_PORT = 7917

    attr_reader :hosts, :logfile, :dryrun
    alias dryrun? dryrun

    def initialize(*hosts)
      options = hosts.last.is_a?(Hash) ? hosts.pop : {}
      @logfile = options[:log] || "/tmp/gizzmo.log"
      @dryrun = options[:dry_run] || false
      @hosts = hosts.flatten
    end

    def get_forwardings(table_id=nil)
      forwardings = client.get_forwardings
      if table_id
        forwardings.select {|f| f.table_id == table_id }
      else
        forwardings
      end
    end

    def reload_forwardings
      all_clients.each {|c| with_retry { c.reload_forwardings } }
    end

    def respond_to?(method)
      client.respond_to? method or super
    end

    def method_missing(method, *args, &block)
      client.respond_to?(method) ? with_retry { client.send(method, *args, &block) } : super
    end

    private

    def client
      @client ||= create_client(hosts.first)
    end

    def all_clients
      @all_clients ||= hosts.map {|host| create_client(host) }
    end

    def create_client(host)
      host, port = host.split(":")
      port ||= DEFAULT_PORT
      Gizzard::Thrift::ShardManager.new(host, port.to_i, logfile, dryrun)
    end

    private

    def with_retry
      times ||= 3
      yield
    rescue ThriftClient::Simple::ThriftException
      times -= 1
      (times < 0) ? raise : retry
    end
  end

  class Manifest
    attr_reader :forwardings, :links, :shards, :existing_shard_ids, :template_map

    def initialize(nameserver, config)
      @config = config
      @forwardings = nameserver.get_forwardings(@config.table_id)
      @links = collect_links(nameserver, forwardings.map {|f| f.shard_id })
      @shards = collect_shards(nameserver, links)

      build_template_map!
    end

    def build_template_map!
      # can't use a default block for these as they wouldn't be marshalable.
      # map[template] #=> [shard_enums...]
      @template_map = {}

      # map[canonical_id] #=> shard_name
      @existing_shard_ids = {}

      forwardings.map{|f| [f.base_id, f.shard_id] }.each do |(base_id, shard_id)|
        enum = shard_id.table_prefix.match(/\d{3,}/)[0].to_i
        tree = build_tree(enum, shard_id, ShardTemplate::DEFAULT_WEIGHT)

        (@template_map[tree] ||= []) << enum
      end
    end

    private

    # FIXME: figure out how to remove the side-effect of adding to the
    # name map
    def build_tree(enum, shard_id, link_weight)
      children = (links[shard_id] || []).map do |(child_id, child_weight)|
        build_tree(enum, child_id, child_weight)
      end

      template = ShardTemplate.from_shard_info(shards[shard_id], link_weight, children)

      canonical_id = template.to_shard_id(@config.shard_name(enum))
      @existing_shard_ids[canonical_id] = shard_id

      template
    end

    def collect_links(nameserver, roots)
      links = {}

      collector = lambda do |parent|
        children = nameserver.list_downward_links(parent).map do |link|
          (links[link.up_id] ||= []) << [link.down_id, link.weight]
          link.down_id
        end

        children.each { |child| collector.call(child) }
      end

      roots.each {|root| collector.call(root) }
      links
    end

    def collect_shards(nameserver, links)
      shard_ids = links.keys + links.values.inject([]) do |ids, nodes|
        nodes.each {|id, weight| ids << id }; ids
      end

      shard_ids.inject({}) {|h, id| h.update id => nameserver.get_shard(id) }
    end
  end
end
