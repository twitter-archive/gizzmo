#!/usr/bin/env ruby
require 'vendor/thrift_client/simple'

module Gizzard
  module Thrift
    T = ThriftClient::Simple

    def self.struct(*args)
      T::StructType.new(*args)
    end

    ShardException = T.make_exception(:ShardException,
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

    ShardMigration = T.make_struct(:ShardMigration,
      T::Field.new(:source_id, struct(ShardId), 1),
      T::Field.new(:destination_id, struct(ShardId), 2)
    )

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

    class GizzmoService < T::ThriftService
      def initialize(host, port, log_path, dry_run = false)
        super(host, port)
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

    class ShardManager < GizzmoService
      thrift_method :create_shard, void, field(:shard, struct(ShardInfo), 1), :throws => exception(ShardException)
      thrift_method :delete_shard, void, field(:id, struct(ShardId), 1), :throws => exception(ShardException)
      thrift_method :get_shard, struct(ShardInfo), field(:id, struct(ShardId), 1), :throws => exception(ShardException)

      thrift_method :add_link, void, field(:up_id, struct(ShardId), 1), field(:down_id, struct(ShardId), 2), field(:weight, i32, 3), :throws => exception(ShardException)
      thrift_method :remove_link, void, field(:up_id, struct(ShardId), 1), field(:down_id, struct(ShardId), 2), :throws => exception(ShardException)

      thrift_method :list_upward_links, list(struct(LinkInfo)), field(:id, struct(ShardId), 1), :throws => exception(ShardException)
      thrift_method :list_downward_links, list(struct(LinkInfo)), field(:id, struct(ShardId), 1), :throws => exception(ShardException)

      thrift_method :get_child_shards_of_class, list(struct(ShardInfo)), field(:parent_id, struct(ShardId), 1), field(:class_name, string, 2), :throws => exception(ShardException)

      thrift_method :mark_shard_busy, void, field(:id, struct(ShardId), 1), field(:busy, i32, 2), :throws => exception(ShardException)
      thrift_method :copy_shard, void, field(:source_id, struct(ShardId), 1), field(:destination_id, struct(ShardId), 2), :throws => exception(ShardException)

      thrift_method :set_forwarding, void, field(:forwarding, struct(Forwarding), 1), :throws => exception(ShardException)
      thrift_method :replace_forwarding, void, field(:old_id, struct(ShardId), 1), field(:new_id, struct(ShardId), 2), :throws => exception(ShardException)
      thrift_method :remove_forwarding, void, field(:forwarding, struct(Forwarding), 1), :throws => exception(ShardException)

      thrift_method :get_forwarding, struct(Forwarding), field(:table_id, i32, 1), field(:base_id, i64, 2), :throws => exception(ShardException)
      thrift_method :get_forwarding_for_shard, struct(Forwarding), field(:shard_id, struct(ShardId), 1), :throws => exception(ShardException)

      thrift_method :get_forwardings, list(struct(Forwarding)), :throws => exception(ShardException)
      thrift_method :reload_forwardings, void, :throws => exception(ShardException)

      thrift_method :find_current_forwarding, struct(ShardInfo), field(:table_id, i32, 1), field(:id, i64, 2), :throws => exception(ShardException)

      thrift_method :shards_for_hostname, list(struct(ShardInfo)), field(:hostname, string, 1), :throws => exception(ShardException)
      thrift_method :get_busy_shards, list(struct(ShardInfo)), :throws => exception(ShardException)
      thrift_method :list_hostnames, list(string), :throws => exception(ShardException)

      thrift_method :rebuild_schema, void, :throws => exception(ShardException)
    end

    class JobManager < GizzmoService
      thrift_method :retry_errors, void
      thrift_method :stop_writes, void
      thrift_method :resume_writes, void
      thrift_method :retry_errors_for, void, field(:priority, i32, 1)
      thrift_method :stop_writes_for, void, field(:priority, i32, 1)
      thrift_method :resume_writes_for, void, field(:priority, i32, 1)
      thrift_method :is_writing, bool, field(:priority, i32, 1)
      thrift_method :inject_job, void, field(:priority, i32, 1), field(:job, string, 2)
    end
  end
end
