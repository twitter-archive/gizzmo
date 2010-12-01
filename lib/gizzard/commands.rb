require "pp"
require "digest/md5"

module Gizzard
  class Command
    include Thrift

    attr_reader :buffer

    def self.run(command_name, global_options, argv, subcommand_options, log, service=nil)
      command_class = Gizzard.const_get("#{classify(command_name)}Command")
      service = command_class.make_service(global_options, log) if service.nil?
      command = command_class.new(service, global_options, argv, subcommand_options)
      command.run
      if command.buffer && command_name = global_options.render.shift
        run(command_name, global_options, command.buffer, OpenStruct.new, log, service)
      end
    end

    def self.classify(string)
      string.split(/\W+/).map { |s| s.capitalize }.join("")
    end

    attr_reader :service, :global_options, :argv, :command_options
    def initialize(service, global_options, argv, command_options)
      @service         = service
      @global_options  = global_options
      @argv            = argv
      @command_options = command_options
    end

    def help!(message = nil)
      raise HelpNeededError, message
    end

    def output(string)
      if global_options.render.any?
        @buffer ||= []
        @buffer << string.strip
      else
        puts string
      end
    end
  end

  class RetryProxy
    def initialize(retries, object)
      @inner = object
      @retries_left = retries
    end

    def method_missing(*args)
      @inner.send(*args)
    rescue
      if @retries_left > 0
        @retries_left -= 1
        STDERR.puts "Retrying..."
        method_missing(*args)
      else
        raise
      end
    end
  end

  class ShardCommand < Command
    def self.make_service(global_options, log)
      RetryProxy.new global_options.retry.to_i, 
        Gizzard::Thrift::ShardManager.new(global_options.host, global_options.port, log, global_options.framed, global_options.dry)
    end
  end

  class JobCommand < Command
    def self.make_service(global_options, log)
      RetryProxy.new global_options.retry.to_i  ,
        Gizzard::Thrift::JobManager.new(global_options.host, global_options.port + 2, log, global_options.framed, global_options.dry)
    end
  end

  class AddforwardingCommand < ShardCommand
    def run
      help! if argv.length != 3
      table_id, base_id, shard_id_text = argv
      shard_id = ShardId.parse(shard_id_text)
      service.set_forwarding(Forwarding.new(table_id.to_i, base_id.to_i, shard_id))
    end
  end

  class DeleteforwardingCommand < ShardCommand
    def run
      help! if argv.length != 3
      table_id, base_id, shard_id_text = argv
      shard_id = ShardId.parse(shard_id_text)
      service.remove_forwarding(Forwarding.new(table_id.to_i, base_id.to_i, shard_id))
    end
  end

  class HostsCommand < ShardCommand
    def run
      service.list_hostnames.map do |host|
        puts host
      end
    end
  end

  class ForwardingsCommand < ShardCommand
    def run
      service.get_forwardings().sort_by do |f|
        [ ((f.table_id.abs << 1) + (f.table_id < 0 ? 1 : 0)), f.base_id ]
      end.reject do |forwarding|
        @command_options.table_ids && !@command_options.table_ids.include?(forwarding.table_id)
      end.each do |forwarding|
        output [ forwarding.table_id, @command_options.hex ? ("%016x" % forwarding.base_id) : forwarding.base_id, forwarding.shard_id.to_unix ].join("\t")
      end
    end
  end

  class SubtreeCommand < ShardCommand
    def run
      @roots = []
      argv.each do |arg|
        @id = ShardId.parse(arg)
        @roots += roots_of(@id)
      end
      @roots.uniq.each do |root|
        output root.to_unix
        down(root, 1)
      end
    end

    def roots_of(id)
      links = service.list_upward_links(id)
      if links.empty?
        [id]
      else
        links.map { |link| roots_of(link.up_id) }.flatten
      end
    end

    def down(id, depth = 0)
      service.list_downward_links(id).map do |link|
        printable = "  " * depth + link.down_id.to_unix
        output printable
        down(link.down_id, depth + 1)
      end
    end
  end

  class ReloadCommand < ShardCommand
    def run
      if global_options.force || ask
        if @argv
          # allow hosts to be given on the command line
          @argv.each do |hostname|
            output hostname
            opts = global_options.dup
            opts.host = hostname
            s = self.class.make_service(opts, global_options.log || "./gizzmo.log")
            s.reload_forwardings
          end
        else
          service.reload_forwardings
        end
      else
        STDERR.puts "aborted"
      end
    end

    def ask
      output "Are you sure? Reloading will affect production services immediately! (Type 'yes')"
      gets.chomp == "yes"
    end
  end

  class DeleteCommand < ShardCommand
    def run
      argv.each do |arg|
        id  = ShardId.parse(arg)
        service.delete_shard(id)
        output id.to_unix
      end
    end
  end

  class AddlinkCommand < ShardCommand
    def run
      up_id, down_id, weight = argv
      help! if argv.length != 3
      weight = weight.to_i
      up_id = ShardId.parse(up_id)
      down_id = ShardId.parse(down_id)
      link = LinkInfo.new(up_id, down_id, weight)
      service.add_link(link.up_id, link.down_id, link.weight)
      output link.to_unix
    end
  end

  class UnlinkCommand < ShardCommand
    def run
      up_id, down_id = argv
      up_id = ShardId.parse(up_id)
      down_id = ShardId.parse(down_id)
      service.remove_link(up_id, down_id)
    end
  end

  class UnwrapCommand < ShardCommand
    def run
      shard_ids = argv
      help! "No shards specified" if shard_ids.empty?
      shard_ids.each do |shard_id_string|
        shard_id = ShardId.parse(shard_id_string)

        upward_links = service.list_upward_links(shard_id)
        downward_links = service.list_downward_links(shard_id)

        if upward_links.length == 0 or downward_links.length == 0
          STDERR.puts "Shard #{shard_id_string} must not be a root or leaf"
          next
        end

        upward_links.each do |uplink|
          downward_links.each do |downlink|
            service.add_link(uplink.up_id, downlink.down_id, uplink.weight)
            new_link = LinkInfo.new(uplink.up_id, downlink.down_id, uplink.weight)
            service.remove_link(uplink.up_id, uplink.down_id)
            service.remove_link(downlink.up_id, downlink.down_id)
            output new_link.to_unix
          end
        end
        service.delete_shard shard_id
      end
    end
  end

  class CreateCommand < ShardCommand
    def run
      help! if argv.length < 2
      class_name, *shard_ids = argv
      busy = 0
      source_type = command_options.source_type || ""
      destination_type = command_options.destination_type || ""
      shard_ids.each do |id|
        shard_id = ShardId.parse(id)
        service.create_shard(ShardInfo.new(shard_id, class_name, source_type, destination_type, busy))
        service.get_shard(shard_id)
        output shard_id.to_unix
      end
    end
  end

  class LinksCommand < ShardCommand
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id_text|
        shard_id = ShardId.parse(shard_id_text)
        next if !shard_id
        unless command_options.down
          service.list_upward_links(shard_id).each do |link_info|
            output command_options.ids ? link_info.up_id.to_unix : link_info.to_unix
          end
        end
        unless command_options.up
          service.list_downward_links(shard_id).each do |link_info|
            output command_options.ids ? link_info.down_id.to_unix : link_info.to_unix
          end
        end
      end
    end
  end

  class InfoCommand < ShardCommand
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id|
        shard_info = service.get_shard(ShardId.parse(shard_id))
        output shard_info.to_unix
      end
    end
  end

  class MarkbusyCommand < ShardCommand
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id|
        id = ShardId.parse(shard_id)
        service.mark_shard_busy(id, 1)
        shard_info = service.get_shard(id)
        output shard_info.to_unix
      end
    end
  end

  class MarkunbusyCommand < ShardCommand
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id|
        id = ShardId.parse(shard_id)
        service.mark_shard_busy(id, 0)
        shard_info = service.get_shard(id)
        output shard_info.to_unix
      end
    end
  end

  class RepairCommand < ShardCommand
    def run
      args = @argv.dup.map{|a| a.split(/\s+/)}.flatten
      pairs = []
      loop do
        a = args.shift
        b = args.shift
        break unless a && b
        pairs << [a, b]
      end
      pairs.each do |master, slave|
        puts "#{master} #{slave}"
        mprefixes = service.shards_for_hostname(master).map{|s| s.id.table_prefix}
        sprefixes = service.shards_for_hostname(slave).map{|s| s.id.table_prefix}
        delta = mprefixes - sprefixes
        delta.each do |prefix|
          puts "gizzmo copy #{master}/#{prefix} #{slave}/#{prefix}"
        end
      end
    end
  end

  class WrapCommand < ShardCommand
    def self.derive_wrapper_shard_id(shard_info, wrapping_class_name)
      suffix = "_" + wrapping_class_name.split(".").last.downcase.gsub("shard", "")
      ShardId.new("localhost", shard_info.id.table_prefix + suffix)
    end

    def run
      class_name, *shard_ids = @argv
      help! "No shards specified" if shard_ids.empty?
      shard_ids.each do |shard_id_string|
        shard_id   = ShardId.parse(shard_id_string)
        shard_info = service.get_shard(shard_id)
        service.create_shard(ShardInfo.new(wrapper_id = self.class.derive_wrapper_shard_id(shard_info, class_name), class_name, "", "", 0))

        existing_links = service.list_upward_links(shard_id)
        unless existing_links.include?(LinkInfo.new(wrapper_id, shard_id, 1))
          service.add_link(wrapper_id, shard_id, 1)
          existing_links.each do |link_info|
            service.add_link(link_info.up_id, wrapper_id, link_info.weight)
            service.remove_link(link_info.up_id, link_info.down_id)
          end
        end
        output wrapper_id.to_unix
      end
    end
  end

  class RebalanceCommand < ShardCommand

    class NamedArray < Array
      attr_reader :name
      def initialize(name)
        @name = name
      end
    end

    def run
      help! "No shards specified" if @argv.empty?
      shards = []
      command_options.write_only_shard ||= "com.twitter.gizzard.shards.WriteOnlyShard"
      additional_hosts = (command_options.hosts || "").split(/[\s,]+/)
      exclude_hosts = (command_options.exclude_hosts || "").split(/[\s,]+/)
      ids = @argv.map{|arg| ShardId.new(*arg.split("/")) rescue nil }.compact
      by_host = ids.inject({}) do |memo, id|
        memo[id.hostname] ||= NamedArray.new(id.hostname)
        memo[id.hostname] << id
        memo
      end

      additional_hosts.each do |host|
        by_host[host] ||= NamedArray.new(host)
      end

      exclude_hosts.each do |host|
        by_host[host] ||= NamedArray.new(host)
      end

      sets = by_host.values
      exclude_sets = exclude_hosts.map{|host| by_host[host]}
      target_sets = sets - exclude_sets

      exclude_sets.each do |set|
        while set.length > 0
          sorted = target_sets.sort_by{|s| s.length}
          shortest = sorted.first
          shortest.push set.pop
        end
      end

      exclude_sets.each do |set|
        while set.length > 0
          sorted = target_sets.sort_by{|s| s.length}
          shortest = sorted.first
          shortest.push set.pop
        end
      end

      begin
        sorted = target_sets.sort_by{|s| s.length }
        longest = sorted.last
        shortest = sorted.first
        shortest.push longest.pop
      end while longest.length > shortest.length + 1

      shard_info = nil
      sets.each do |set|
        host = set.name
        set.each do |id|
          if id.hostname != host
            shard_info ||= service.get_shard(id)
            old = id.to_unix
            id.hostname = host
            shards << [old, id.to_unix]
          end
        end
      end

      new_shards = shards.map{|(old, new)| new }

      puts "gizzmo create #{shard_info.class_name} -s '#{shard_info.source_type}' -d '#{shard_info.destination_type}' #{new_shards.join(" ")}"
      puts "gizzmo wrap #{command_options.write_only_shard} #{new_shards.join(" ")}"
      shards.map { |(old, new)| puts "gizzmo copy #{old} #{new}" }
    end
  end

  class PairCommand < ShardCommand
    def run
      ids = []
      @argv.map do |host|
        service.shards_for_hostname(host).each do |shard|
          ids << shard.id
        end
      end

      ids_by_table = {}
      ids.map do |id|
        ids_by_table[id.table_prefix] ||= []
        ids_by_table[id.table_prefix] << id
      end

      ids_by_host = {}
      ids.map do |id|
        ids_by_host[id.hostname] ||= []
        ids_by_host[id.hostname] << id
      end

      overlaps = {}
      ids_by_table.values.each do |arr|
        key = arr.map{|id| id.hostname }.sort
        overlaps[key] ||= 0
        overlaps[key]  += 1
      end

      displayed = {}
      overlaps.sort_by{|hosts, count| count }.reverse.each do |(host_a, host_b), count|
        next if !host_a || !host_b || displayed[host_a] || displayed[host_b]
        id_a = ids_by_host[host_a].find{|id| service.list_upward_links(id).size > 0 }
        id_b = ids_by_host[host_b].find{|id| service.list_upward_links(id).size > 0 }
        next unless id_a && id_b
        weight_a = service.list_upward_links(id_a).first.weight
        weight_b = service.list_upward_links(id_b).first.weight
        if weight_a > weight_b
          puts "#{host_a}\t#{host_b}"
        else
          puts "#{host_b}\t#{host_a}"
        end
        displayed[host_a] = true
        displayed[host_b] = true
      end
      remaining = @argv - displayed.keys
      loop do
        a = remaining.shift
        b = remaining.shift
        break unless a && b
        puts "#{a}\t#{b}"
      end
    end
  end

  class ReportCommand < ShardCommand
    def run
      things = @argv.map do |shard|
        parse(down(ShardId.parse(shard))).join("\n")
      end

      if command_options.flat
        things.zip(@argv).each do |thing, shard_id|
          puts "#{sign(thing)}\t#{shard_id}"
        end
      else
        group(things).each do |string, things|
          puts "=== " + sign(string) + ": #{things.length}" + " ===================="
          puts string
        end
      end
    end

    def sign(string)
      ::Digest::MD5.hexdigest(string)[0..10]
    end

    def group(arr)
      arr.inject({}) do |m, e|
        m[e] ||= []
        m[e] << e
        m
      end.to_a.sort_by{|k, v| v.length}.reverse
    end

    def parse(obj, id = nil, depth = 0, sub = true)
      case obj
      when Hash
        id, prefix = parse(obj.keys.first, id, depth, sub)
        [prefix] + parse(obj.values.first, id, depth + 1, sub)
      when String
        host, prefix = obj.split("/")
        host = "db" if host != "localhost" && sub
        id ||= prefix[/(\w+ward_)?\d+_\d+(_\w+ward)?/]
        prefix = ("  " * depth) + host + "/" + ((sub && id) ? prefix.sub(id, "[ID]") : prefix)
        [id, prefix]
      when Array
        obj.map do |e|
          parse e, id, depth, sub
        end
      end
    end

    def down(id)
      vals = service.list_downward_links(id).map do |link|
        down(link.down_id)
      end
      {id.to_unix => vals}
    end
  end

  class DrillCommand < ReportCommand
    def run
      signature = @argv.shift
      @argv.map do |shard|
        if sign(parse(down(ShardId.parse(shard))).join("\n")) == signature
          puts parse(down(ShardId.parse(shard)), nil, 0, false).join("\n")
        end
      end
    end
  end

  class FindCommand < ShardCommand
    def run
      hosts = @argv << command_options.shard_host
      hosts.compact.each do |host|
        service.shards_for_hostname(host).each do |shard|
          next if command_options.shard_type && shard.class_name !~ Regexp.new(command_options.shard_type)
          output shard.id.to_unix
        end
      end
    end
  end

  class LookupCommand < ShardCommand
    def run
      table_id, source = @argv
      help!("Requires table id and source") unless table_id && source
      case @command_options.hash_function
      when :fnv
        source_id = Digest.fnv1a_64(source)
      else
        source_id = source.to_i
      end
      shard = service.find_current_forwarding(table_id.to_i, source_id)
      output shard.id.to_unix
    end
  end

  class CopyCommand < ShardCommand
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)
      service.copy_shard(from_shard_id, to_shard_id)
    end
  end

  class BusyCommand < ShardCommand
    def run
      service.get_busy_shards().each { |shard_info| output shard_info.to_unix }
    end
  end

  class SetupMigrateCommand < ShardCommand
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)

      if service.list_upward_links(to_shard_id).size > 0
        STDERR.puts "Destination shard #{to_shard_id} has links to it."
        exit 1
      end

      write_only_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_write_only")
      replica_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_replica")
      service.create_shard(ShardInfo.new(write_only_shard_id, "com.twitter.gizzard.shards.WriteOnlyShard", "", "", 0))
      service.create_shard(ShardInfo.new(replica_shard_id, "com.twitter.gizzard.shards.ReplicatingShard", "", "", 0))
      service.add_link(write_only_shard_id, to_shard_id, 1)
      service.list_upward_links(from_shard_id).each do |link|
        service.remove_link(link.up_id, link.down_id)
        service.add_link(link.up_id, replica_shard_id, link.weight)
      end
      service.add_link(replica_shard_id, from_shard_id, 1)
      service.add_link(replica_shard_id, write_only_shard_id, 0)
      service.replace_forwarding(from_shard_id, replica_shard_id)
      output replica_shard_id.to_unix
    end
  end

  class FinishMigrateCommand < ShardCommand
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)

      write_only_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_write_only")
      replica_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_replica")

      # careful. need to validate some basic assumptions.
      unless global_options.force
        if service.list_upward_links(from_shard_id).map { |link| link.up_id }.to_a != [ replica_shard_id ]
          STDERR.puts "Uplink from #{from_shard_id} is not a migration replica."
          exit 1
        end
        if service.list_upward_links(to_shard_id).map { |link| link.up_id }.to_a != [ write_only_shard_id ]
          STDERR.puts "Uplink from #{to_shard_id} is not a write-only barrier."
          exit 1
        end
        if service.list_upward_links(write_only_shard_id).map { |link| link.up_id }.to_a != [ replica_shard_id ]
          STDERR.puts "Uplink from write-only barrier is not a migration replica."
          exit 1
        end
      end

      service.remove_link(write_only_shard_id, to_shard_id)
      service.list_upward_links(replica_shard_id).each do |link|
        service.remove_link(link.up_id, link.down_id)
        service.add_link(link.up_id, to_shard_id, link.weight)
      end
      service.replace_forwarding(replica_shard_id, to_shard_id)
      service.delete_shard(replica_shard_id)
      service.delete_shard(write_only_shard_id)
    end
  end

  class InjectCommand < JobCommand
    def run
      priority, *jobs = @argv
      help!("Requires priority") unless priority and jobs.size > 0
      count = 0
      jobs.each do |job|
        service.inject_job(priority.to_i, job)
        count += 1
        # FIXME add -q --quiet option
        STDERR.print "."
        STDERR.print "#{count}" if count % 100 == 0
        STDERR.flush
      end
      STDERR.print "\n"
    end
  end

  class FlushCommand < JobCommand
    def run
      args = @argv[0]
      help!("Requires --all, or a job priority id.") unless args || command_options.flush_all
      if command_options.flush_all
        service.retry_errors()
      else
        service.retry_errors_for(args.to_i)
      end
    end
  end
end
