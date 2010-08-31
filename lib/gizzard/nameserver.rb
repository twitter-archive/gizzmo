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


  class MockNameserver
    def initialize(*hosts)
    end

    def get_shard(id)
      Gizzard::Thrift::ShardInfo.new(id, "", "", "", 0)
    end

    def reload_forwardings
    end

    def method_missing(method, *args)
      puts "#{method.to_s.upcase}:\t#{args.map{|a| a.inspect }.join(",\t")}"
    end
  end
end
