module Gizzard
  class Nameserver

    DEFAULT_PORT = 7917
    RETRIES = 3

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
      times ||= RETRIES
      yield
    rescue ThriftClient::Simple::ThriftException
      times -= 1
      (times < 0) ? raise : (sleep 0.1; retry)
    end
  end

  Shard = Struct.new(:info, :children, :weight)

  class Shard
    def id; info.id; end
  end

  class Manifest
    attr_reader :forwardings, :links, :shard_infos, :trees, :template_map

    def initialize(nameserver, config)
      @config = config
      @forwardings = collect_forwardings(nameserver, @config.table_id)
      @links = collect_links(nameserver, forwardings.map {|f| f.shard_id })
      @shard_infos = collect_shard_infos(nameserver, links)
      @trees = forwardings.inject({}) do |h, forwarding|
        h.update forwarding => build_tree(forwarding.shard_id)
      end

      @template_map = @trees.inject do |h, (forwarding, shard)|
        (h[build_template(shard)] ||= []) << forwarding; h
      end
    end

    private

    def build_tree(shard_id, link_weight = nil)
      children = (links[shard_id] || []).map do |(child_id, child_weight)|
        build_tree(child_id, child_weight)
      end

      info = shard_infos[shard_id] or raise "shard info not found for: #{shard_id}"
      Shard.new(info, children, link_weight)
    end

    def build_template(shard)
      children = shard.children.map do |child|
        build_template(child)
      end

      ShardTemplate.new(shard.info.class_name,
                        shard.id.hostname,
                        shard.weight,
                        shard.info.source_type,
                        shard.info.destination_type,
                        children)
    end

    def collect_forwardings(nameserver, table_id)
      nameserver.get_forwardings(table_id)
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

    def collect_shard_infos(nameserver, links)
      shard_ids = links.keys + links.values.inject([]) do |ids, nodes|
        nodes.each {|id, weight| ids << id }; ids
      end

      shard_ids.inject({}) {|h, id| h.update id => nameserver.get_shard(id) }
    end
  end
end
