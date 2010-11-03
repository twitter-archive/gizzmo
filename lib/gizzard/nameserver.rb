module Gizzard
  class Nameserver

    DEFAULT_PORT = 7917
    RETRIES = 3
    PARALLELISM = 20

    attr_reader :hosts, :logfile, :dryrun
    alias dryrun? dryrun

    def initialize(*hosts)
      options = hosts.last.is_a?(Hash) ? hosts.pop : {}
      @logfile = options[:log] || "/tmp/gizzmo.log"
      @dryrun = options[:dry_run] || false
      @hosts = hosts.flatten
    end

    def get_all_links(forwardings=nil)
      mutex         = Mutex.new
      all_links     = {}
      forwardings ||= client.get_forwardings
      forwardings   = forwardings.dup

      Thread.abort_on_exception = true

      threads = (0..(PARALLELISM - 1)).map do |i|
        Thread.new do
          done   = {}
          client = create_client(hosts.first)

          while f = mutex.synchronize { forwardings.pop }
            pending = [f.shard_id]

            until pending.empty?
              id = pending.pop

              unless done[id]
                links = with_retry { client.list_downward_links id }
                links.each {|l| pending << l.down_id }
                mutex.synchronize { links.each {|l| all_links[l] = true } }
                done[id] = true
              end
            end
          end
        end
      end

      threads.each {|t| t.join }

      all_links.keys
    end

    def get_all_shards
      client.list_hostnames.inject([]) do |a, hostname|
        a.concat client.shards_for_hostname(hostname)
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

    def manifest
      Manifest.new(self)
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


    # manifest helper class
    Shard = Struct.new(:info, :children, :weight)

    class Shard
      def id; info.id; end
    end

    class Manifest
      attr_reader :forwardings, :links, :shard_infos, :trees, :templates

      def initialize(nameserver)
        @forwardings = nameserver.get_forwardings

        @links = nameserver.get_all_links(forwardings).inject({}) do |h, link|
          (h[link.up_id] ||= []) << [link.down_id, link.weight]; h
        end

        @shard_infos = nameserver.get_all_shards.inject({}) do |h, shard|
          h.update shard.id => shard
        end

        @trees = forwardings.inject({}) do |h, forwarding|
          h.update forwarding => build_tree(forwarding.shard_id)
        end

        @templates = @trees.inject({}) do |h, (forwarding, shard)|
          (h[build_template(shard)] ||= []) << forwarding; h
        end
      end

      private

      def build_tree(shard_id, link_weight=ShardTemplate::DEFAULT_WEIGHT)
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
    end
  end
end
