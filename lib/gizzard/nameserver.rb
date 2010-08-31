module Gizzard
  class Nameserver
    attr_reader :hosts, :logfile, :dryrun
    alias dryrun? dryrun

    def initialize(*hosts)
      options = hosts.last.is_a?(Hash) ? hosts.pop : {}
      @logfile = options[:log] || "/tmp/gizzmo.log"
      @dryrun = options[:dry_run] || false
      @hosts = hosts.flatten
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
      Gizzard::Thrift::ShardManager.new(host, port.to_i, logfile, dryrun)
    end

    private

    def with_retry
      times ||= 3
      yield
    rescue ThriftClient::Simple::ThriftException
      times -= 1
      times < 0 ? raise : retry
    end
  end

  class Manifest
    attr_reader :forwardings, :links, :shards, :prefix_translation_map

    def initialize(nameserver)
      @forwardings = nameserver.get_forwardings
      @links = collect_links(nameserver, forwardings.map {|f| f.shard_id })
      @shards = collect_shards(nameserver, links)
      @prefix_translation_map = @shards.inject({}) do |m, (id, info)|
        #TODO generate translation map here
      end
    end

    private

    def collect_links(nameserver, roots)
      links = Hash.new {|h,k| h[k] = [] }

      collector = lambda do |parent|
        children = nameserver.list_downward_links(parent).map do |link|
          links[link.up_id] << [link.down_id, link.weight]
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
