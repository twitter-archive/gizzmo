require 'vendor/thrift_client/simple'

module Gizzard
  T = ThriftClient::Simple

  def self.struct(*args)
    T::StructType.new(*args)
  end

  GizzardException = T.make_exception(:GizzardException,
    T::Field.new(:description, T::STRING, 1)
  )

  ShardId = T.make_struct(:ShardId,
    T::Field.new(:hostname, T::STRING, 1),
    T::Field.new(:table_prefix, T::STRING, 2)
  )

  class ShardId
    def inspect
      "#{hostname}/#{table_prefix}"
    end

    def <=>(o)
      self.hostname <=> o.hostname
    end

    alias_method :to_unix, :inspect

    def self.parse(string)
      new(*string.split("/"))
    end
  end

  ShardInfo = T.make_struct(:ShardInfo,
    T::Field.new(:id, struct(ShardId), 1),
    T::Field.new(:class_name, T::STRING, 2),
    T::Field.new(:source_type, T::STRING, 3),
    T::Field.new(:destination_type, T::STRING, 4),
    T::Field.new(:busy, T::I32, 5)
  )

  class ShardInfo
    def busy?
      busy && busy > 0
    end

    def inspect(short = false)
      "#{id.inspect}" + (busy? ? " (BUSY)" : "")
    end

    def to_unix
      [id.to_unix, class_name, busy? ? "busy" : "unbusy"].join("\t")
    end
  end

  LinkInfo = T.make_struct(:LinkInfo,
    T::Field.new(:up_id, struct(ShardId), 1),
    T::Field.new(:down_id, struct(ShardId), 2),
    T::Field.new(:weight, T::I32, 3)
  )

  class LinkInfo
    def inspect
      "#{up_id.inspect} -> #{down_id.inspect}" + (weight == 1 ? "" : " <#{weight}>")
    end

    def to_unix
      [up_id.to_unix, down_id.to_unix, weight].join("\t")
    end
  end

  Forwarding = T.make_struct(:Forwarding,
    T::Field.new(:table_id, T::I32, 1),
    T::Field.new(:base_id, T::I64, 2),
    T::Field.new(:shard_id, struct(ShardId), 3)
  )

  class Forwarding
    #FIXME table_id is not human-readable
    def inspect
      "[#{table_id}] #{base_id.to_s(16)} -> #{shard_id.inspect}"
    end
  end

  module HostStatus
    Normal  = 0
    Offline = 1
    Blocked = 2
  end

  Host = T.make_struct(:Host,
    T::Field.new(:hostname, T::STRING, 1),
    T::Field.new(:port, T::I32, 2),
    T::Field.new(:cluster, T::STRING, 3),
    T::Field.new(:status, T::I32, 4)
  )

  class Host
    def inspect
      "(#{hostname}:#{port} - #{cluster} (#{status})"
    end
  end

  class GizzmoService < T::ThriftService
    def initialize(host, port, log_path, framed, dry_run = false)
      super(host, port, framed)
      @dry = dry_run
      begin
        @log = File.open(log_path, "a")
      rescue
        STDERR.puts "Error opening log file at #{log_path}.  Continuing..."
      end
    end

    def _proxy(method_name, *args)
      cls = self.class.ancestors.find { |cls| cls.respond_to?(:_arg_structs) and cls._arg_structs[method_name.to_sym] }
      arg_class, rv_class = cls._arg_structs[method_name.to_sym]

      # Writing methods return void. Methods should never both read and write. If this assumption
      # is violated in the future, dry-run will fail!!
      is_writing_method = rv_class._fields.first.type == ThriftClient::Simple::VOID
      if @dry && is_writing_method
        puts "Skipped writing: #{printable(method_name, args)}"
      else
        @log.puts printable(method_name, args, true)
        super(method_name, *args)
      end
    rescue ThriftClient::Simple::ThriftException
      if @dry
        puts "Skipped reading: #{printable(method_name, args)}"
      else
        raise
      end
    end

    def printable(method_name, args, timestamp = false)
      ts = timestamp ? "#{Time.now}\t" : ""
      "#{ts}#{method_name}(#{args.map{|a| a.inspect}.join(', ')})"
    end
  end

  class Manager < GizzmoService
    thrift_method :reload_config, void, :throws => exception(GizzardException)
    thrift_method :rebuild_schema, void, :throws => exception(GizzardException)

    thrift_method :find_current_forwarding, struct(ShardInfo), field(:table_id, i32, 1), field(:id, i64, 2), :throws => exception(GizzardException)


    # Shard Tree Management

    thrift_method :create_shard, void, field(:shard, struct(ShardInfo), 1), :throws => exception(GizzardException)
    thrift_method :delete_shard, void, field(:id, struct(ShardId), 1), :throws => exception(GizzardException)

    thrift_method :add_link, void, field(:up_id, struct(ShardId), 1), field(:down_id, struct(ShardId), 2), field(:weight, i32, 3), :throws => exception(GizzardException)
    thrift_method :remove_link, void, field(:up_id, struct(ShardId), 1), field(:down_id, struct(ShardId), 2), :throws => exception(GizzardException)

    thrift_method :set_forwarding, void, field(:forwarding, struct(Forwarding), 1), :throws => exception(GizzardException)
    thrift_method :replace_forwarding, void, field(:old_id, struct(ShardId), 1), field(:new_id, struct(ShardId), 2), :throws => exception(GizzardException)
    thrift_method :remove_forwarding, void, field(:forwarding, struct(Forwarding), 1), :throws => exception(GizzardException)

    thrift_method :get_shard, struct(ShardInfo), field(:id, struct(ShardId), 1), :throws => exception(GizzardException)
    thrift_method :shards_for_hostname, list(struct(ShardInfo)), field(:hostname, string, 1), :throws => exception(GizzardException)
    thrift_method :get_busy_shards, list(struct(ShardInfo)), :throws => exception(GizzardException)

    thrift_method :list_upward_links, list(struct(LinkInfo)), field(:id, struct(ShardId), 1), :throws => exception(GizzardException)
    thrift_method :list_downward_links, list(struct(LinkInfo)), field(:id, struct(ShardId), 1), :throws => exception(GizzardException)
    thrift_method :get_forwarding, struct(Forwarding), field(:table_id, i32, 1), field(:base_id, i64, 2), :throws => exception(GizzardException)
    thrift_method :get_forwarding_for_shard, struct(Forwarding), field(:shard_id, struct(ShardId), 1), :throws => exception(GizzardException)
    thrift_method :get_forwardings, list(struct(Forwarding)), :throws => exception(GizzardException)

    thrift_method :list_hostnames, list(string), :throws => exception(GizzardException)

    thrift_method :mark_shard_busy, void, field(:id, struct(ShardId), 1), field(:busy, i32, 2), :throws => exception(GizzardException)
    thrift_method :copy_shard, void, field(:source_id, struct(ShardId), 1), field(:destination_id, struct(ShardId), 2), :throws => exception(GizzardException)


    # Job Scheduler Management

    thrift_method :retry_errors, void
    thrift_method :stop_writes, void
    thrift_method :resume_writes, void

    thrift_method :retry_errors_for, void, field(:priority, i32, 1)
    thrift_method :stop_writes_for, void, field(:priority, i32, 1)
    thrift_method :resume_writes_for, void, field(:priority, i32, 1)

    thrift_method :is_writing, bool, field(:priority, i32, 1)


    # Remote Host Cluster Management

    thrift_method :add_remote_host, void, field(:host, struct(Host), 1)#, :throws => exception(GizzardException)
    thrift_method :remove_remote_host, void, field(:hostname, string, 1), field(:port, i32, 2), :throws => exception(GizzardException)
    thrift_method :set_remote_host_status, void, field(:hostname, string, 1), field(:port, i32, 2), field(:status, i32, 3), :throws => exception(GizzardException)
    thrift_method :set_remote_cluster_status, void, field(:cluster, string, 1), field(:status, i32, 2), :throws => exception(GizzardException)

    thrift_method :get_remote_host, struct(Host), field(:hostname, string, 1), field(:port, i32, 2), :throws => exception(GizzardException)
    thrift_method :list_remote_clusters, list(string), :throws => exception(GizzardException)
    thrift_method :list_remote_hosts, list(struct(Host)), :throws => exception(GizzardException)
    thrift_method :list_remote_hosts_in_cluster, list(struct(Host)), field(:cluster, string, 1), :throws => exception(GizzardException)
  end



  Job = T.make_struct(:Job,
    T::Field.new(:priority, T::I32, 1),
    T::Field.new(:contents, T::STRING, 2)
  )

  JobException = T.make_exception(:JobException,
    T::Field.new(:description, T::STRING, 1)
  )

  class JobInjector < GizzmoService
    thrift_method :inject_jobs, void, field(:priority, list(struct(Job)), 1), :throws => exception(JobException)
  end
end
