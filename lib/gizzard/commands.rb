require "pp"
require "set"
require "digest/md5"

module Gizzard
  class Command

    attr_reader :buffer

    class << self
      def run(command_name, global_options, argv, subcommand_options, log)
        command_class = Gizzard.const_get("#{classify(command_name)}Command")

        @manager      ||= make_manager(global_options, log)
        @job_injector ||= make_job_injector(global_options, log)

        command = command_class.new(@manager, @job_injector, global_options, argv, subcommand_options)
        command.run

        if command.buffer && command_name = global_options.render.shift
          run(command_name, global_options, command.buffer, OpenStruct.new, log)
        end
      end

      def classify(string)
        string.split(/\W+/).map{|s| s.capitalize }.join("")
      end

      def make_manager(global_options, log)
        hosts = global_options.hosts.map {|h| [h, global_options.port].join(":") }

        Nameserver.new(hosts, :retries => global_options.retry,
                              :log     => log,
                              :framed  => global_options.framed,
                              :dry_run => global_options.dry)
      end

      def make_job_injector(global_options, log)
        RetryProxy.new global_options.retry,
          JobInjector.new(global_options.hosts.first, global_options.injector_port, log, true, global_options.dry)
      end
    end

    attr_reader :manager, :job_injector, :global_options, :argv, :command_options

    def initialize(manager, job_injector, global_options, argv, command_options)
      @manager      = manager
      @job_injector    = job_injector
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

  class AddforwardingCommand < Command
    def run
      help! if argv.length != 3
      table_id, base_id, shard_id_text = argv
      shard_id = ShardId.parse(shard_id_text)
      manager.set_forwarding(Forwarding.new(table_id.to_i, base_id.to_i, shard_id))
    end
  end

  class DumpCommand < Command
    def run
      table_ids = argv.map{|e| e.to_i }
      manifest = manager.manifest(*table_ids)
      manifest.trees.values.each do |tree|
        down(tree, 0)
      end
    end

    def down(shard, depth)
      printable = "  " * depth + shard.info.id.to_unix
      output printable
      shard.children.each do |child|
        down(child, depth + 1)
      end
    end
  end

  class DeleteforwardingCommand < Command
    def run
      help! if argv.length != 3
      table_id, base_id, shard_id_text = argv
      shard_id = ShardId.parse(shard_id_text)
      manager.remove_forwarding(Forwarding.new(table_id.to_i, base_id.to_i, shard_id))
    end
  end

  class HostsCommand < Command
    def run
      manager.list_hostnames.map do |host|
        puts host
      end
    end
  end

  class ForwardingsCommand < Command
    def run
      manager.get_forwardings.sort_by do |f|
        [ ((f.table_id.abs << 1) + (f.table_id < 0 ? 1 : 0)), f.base_id ]
      end.reject do |forwarding|
        @command_options.table_ids && !@command_options.table_ids.include?(forwarding.table_id)
      end.each do |forwarding|
        output [ forwarding.table_id, @command_options.hex ? ("%016x" % forwarding.base_id) : forwarding.base_id, forwarding.shard_id.to_unix ].join("\t")
      end
    end
  end

  class SubtreeCommand < Command
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
      links = manager.list_upward_links(id)
      if links.empty?
        [id]
      else
        links.map { |link| roots_of(link.up_id) }.flatten
      end
    end

    def down(id, depth = 0)
      manager.list_downward_links(id).map do |link|
        printable = "  " * depth + link.down_id.to_unix
        output printable
        down(link.down_id, depth + 1)
      end
    end
  end

  class ReloadCommand < Command
    def run
      if global_options.force || ask
        manager.reload_config
      else
        STDERR.puts "aborted"
      end
    end

    def ask
      output "Are you sure? Reloading will affect production services immediately! (Type 'yes')"
      gets.chomp == "yes"
    end
  end

  class DeleteCommand < Command
    def run
      argv.each do |arg|
        id  = ShardId.parse(arg)
        manager.delete_shard(id)
        output id.to_unix
      end
    end
  end

  class AddlinkCommand < Command
    def run
      up_id, down_id, weight = argv
      help! if argv.length != 3
      weight = weight.to_i
      up_id = ShardId.parse(up_id)
      down_id = ShardId.parse(down_id)
      link = LinkInfo.new(up_id, down_id, weight)
      manager.add_link(link.up_id, link.down_id, link.weight)
      output link.to_unix
    end
  end

  class UnlinkCommand < Command
    def run
      up_id, down_id = argv
      up_id = ShardId.parse(up_id)
      down_id = ShardId.parse(down_id)
      manager.remove_link(up_id, down_id)
    end
  end

  class UnwrapCommand < Command
    def run
      shard_ids = argv
      help! "No shards specified" if shard_ids.empty?
      shard_ids.each do |shard_id_string|
        shard_id = ShardId.parse(shard_id_string)

        upward_links = manager.list_upward_links(shard_id)
        downward_links = manager.list_downward_links(shard_id)

        if upward_links.length == 0 or downward_links.length == 0
          STDERR.puts "Shard #{shard_id_string} must not be a root or leaf"
          next
        end

        upward_links.each do |uplink|
          downward_links.each do |downlink|
            manager.add_link(uplink.up_id, downlink.down_id, uplink.weight)
            new_link = LinkInfo.new(uplink.up_id, downlink.down_id, uplink.weight)
            manager.remove_link(uplink.up_id, uplink.down_id)
            manager.remove_link(downlink.up_id, downlink.down_id)
            output new_link.to_unix
          end
        end
        manager.delete_shard shard_id
      end
    end
  end

  class CreateCommand < Command
    def run
      help! if argv.length < 2
      class_name, *shard_ids = argv
      busy = 0
      source_type = command_options.source_type || ""
      destination_type = command_options.destination_type || ""
      shard_ids.each do |id|
        shard_id = ShardId.parse(id)
        manager.create_shard(ShardInfo.new(shard_id, class_name, source_type, destination_type, busy))
        manager.get_shard(shard_id)
        output shard_id.to_unix
      end
    end
  end

  class LinksCommand < Command
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id_text|
        shard_id = ShardId.parse(shard_id_text)
        next if !shard_id
        unless command_options.down
          manager.list_upward_links(shard_id).each do |link_info|
            output command_options.ids ? link_info.up_id.to_unix : link_info.to_unix
          end
        end
        unless command_options.up
          manager.list_downward_links(shard_id).each do |link_info|
            output command_options.ids ? link_info.down_id.to_unix : link_info.to_unix
          end
        end
      end
    end
  end

  class InfoCommand < Command
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id|
        shard_info = manager.get_shard(ShardId.parse(shard_id))
        output shard_info.to_unix
      end
    end
  end

  class MarkbusyCommand < Command
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id|
        id = ShardId.parse(shard_id)
        manager.mark_shard_busy(id, 1)
        shard_info = manager.get_shard(id)
        output shard_info.to_unix
      end
    end
  end

  class MarkunbusyCommand < Command
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id|
        id = ShardId.parse(shard_id)
        manager.mark_shard_busy(id, 0)
        shard_info = manager.get_shard(id)
        output shard_info.to_unix
      end
    end
  end

  class WrapCommand < Command
    def self.derive_wrapper_shard_id(shard_info, wrapping_class_name)
      suffix = "_" + wrapping_class_name.split(".").last.downcase.gsub("shard", "")
      ShardId.new("localhost", shard_info.id.table_prefix + suffix)
    end

    def run
      class_name, *shard_ids = @argv
      help! "No shards specified" if shard_ids.empty?
      shard_ids.each do |shard_id_string|
        shard_id   = ShardId.parse(shard_id_string)
        shard_info = manager.get_shard(shard_id)
        manager.create_shard(ShardInfo.new(wrapper_id = self.class.derive_wrapper_shard_id(shard_info, class_name), class_name, "", "", 0))

        existing_links = manager.list_upward_links(shard_id)
        unless existing_links.include?(LinkInfo.new(wrapper_id, shard_id, 1))
          manager.add_link(wrapper_id, shard_id, 1)
          existing_links.each do |link_info|
            manager.add_link(link_info.up_id, wrapper_id, link_info.weight)
            manager.remove_link(link_info.up_id, link_info.down_id)
          end
        end
        output wrapper_id.to_unix
      end
    end
  end

  class PairCommand < Command
    def run
      ids = []
      @argv.map do |host|
        manager.shards_for_hostname(host).each do |shard|
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
        key = arr.map { |id| id.hostname }.sort
        overlaps[key] ||= 0
        overlaps[key] += 1
      end

      displayed = {}
      overlaps.sort_by { |hosts, count| count }.reverse.each do |(host_a, host_b), count|
        next if !host_a || !host_b || displayed[host_a] || displayed[host_b]
        id_a = ids_by_host[host_a].find {|id| manager.list_upward_links(id).size > 0 }
        id_b = ids_by_host[host_b].find {|id| manager.list_upward_links(id).size > 0 }
        next unless id_a && id_b
        weight_a = manager.list_upward_links(id_a).first.weight
        weight_b = manager.list_upward_links(id_b).first.weight
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

  class ReportCommand < Command
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
      end.to_a.sort_by { |k, v| v.length }.reverse
    end

    def parse(obj, id = nil, depth = 0, sub = true)
      case obj
      when Hash
        id, prefix = parse(obj.keys.first, id, depth, sub)
        [ prefix ] + parse(obj.values.first, id, depth + 1, sub)
      when String
        host, prefix = obj.split("/")
        host = "db" if host != "localhost" && sub
        id ||= prefix[/(\w+ward_)?n?\d+_\d+(_\w+ward)?/]
        prefix = ("  " * depth) + host + "/" + ((sub && id) ? prefix.sub(id, "[ID]") : prefix)
        [ id, prefix ]
      when Array
        obj.map do |e|
          parse e, id, depth, sub
        end
      end
    end

    def down(id)
      vals = manager.list_downward_links(id).map do |link|
        down(link.down_id)
      end
      { id.to_unix => vals }
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

  class FindCommand < Command
    def run
      hosts = @argv << command_options.shard_host
      hosts.compact.each do |host|
        manager.shards_for_hostname(host).each do |shard|
          next if command_options.shard_type && shard.class_name !~ Regexp.new(command_options.shard_type)
          output shard.id.to_unix
        end
      end
    end
  end

  class LookupCommand < Command
    def run
      table_id, source = @argv
      help!("Requires table id and source") unless table_id && source
      case @command_options.hash_function
      when :fnv
        source_id = Digest.fnv1a_64(source)
      else
        source_id = source.to_i
      end
      shard = manager.find_current_forwarding(table_id.to_i, source_id)
      output shard.id.to_unix
    end
  end

  class CopyCommand < Command
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)
      manager.copy_shard(from_shard_id, to_shard_id)
    end
  end

  class BusyCommand < Command
    def run
      manager.get_busy_shards().each { |shard_info| output shard_info.to_unix }
    end
  end

  class SetupReplicaCommand < Command
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)

      if manager.list_upward_links(to_shard_id).size > 0
        STDERR.puts "Destination shard #{to_shard_id} has links to it."
        exit 1
      end

      link = manager.list_upward_links(from_shard_id)[0]
      replica_shard_id = link.up_id
      weight = link.weight
      write_only_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_copy_write_only")
      manager.create_shard(ShardInfo.new(write_only_shard_id, "WriteOnlyShard", "", "", 0))
      manager.add_link(replica_shard_id, write_only_shard_id, weight)
      manager.add_link(write_only_shard_id, to_shard_id, 1)
      output to_shard_id.to_unix
    end
  end

  class FinishReplicaCommand < Command
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)

      write_only_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_copy_write_only")
      link = manager.list_upward_links(write_only_shard_id)[0]
      replica_shard_id = link.up_id
      weight = link.weight

      # careful. need to validate some basic assumptions.
      unless global_options.force
        if manager.list_upward_links(from_shard_id).map { |link| link.up_id }.to_a != [ replica_shard_id ]
          STDERR.puts "Uplink from #{from_shard_id} is not a migration replica."
          exit 1
        end
        if manager.list_upward_links(to_shard_id).map { |link| link.up_id }.to_a != [ write_only_shard_id ]
          STDERR.puts "Uplink from #{to_shard_id} is not a write-only barrier."
          exit 1
        end
      end

      manager.remove_link(write_only_shard_id, to_shard_id)
      manager.remove_link(replica_shard_id, write_only_shard_id)
      manager.add_link(replica_shard_id, to_shard_id, weight)
      manager.delete_shard(write_only_shard_id)
    end
  end

  class SetupMigrateCommand < Command
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)

      if manager.list_upward_links(to_shard_id).size > 0
        STDERR.puts "Destination shard #{to_shard_id} has links to it."
        exit 1
      end

      write_only_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_write_only")
      replica_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_replica")
      manager.create_shard(ShardInfo.new(write_only_shard_id, "com.twitter.gizzard.shards.WriteOnlyShard", "", "", 0))
      manager.create_shard(ShardInfo.new(replica_shard_id, "com.twitter.gizzard.shards.ReplicatingShard", "", "", 0))
      manager.add_link(write_only_shard_id, to_shard_id, 1)
      manager.list_upward_links(from_shard_id).each do |link|
        manager.remove_link(link.up_id, link.down_id)
        manager.add_link(link.up_id, replica_shard_id, link.weight)
      end
      manager.add_link(replica_shard_id, from_shard_id, 1)
      manager.add_link(replica_shard_id, write_only_shard_id, 0)
      manager.replace_forwarding(from_shard_id, replica_shard_id)
      output replica_shard_id.to_unix
    end
  end

  class FinishMigrateCommand < Command
    def run
      from_shard_id_string, to_shard_id_string = @argv
      help!("Requires source & destination shard id") unless from_shard_id_string && to_shard_id_string
      from_shard_id = ShardId.parse(from_shard_id_string)
      to_shard_id = ShardId.parse(to_shard_id_string)

      write_only_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_write_only")
      replica_shard_id = ShardId.new("localhost", "#{to_shard_id.table_prefix}_migrate_replica")

      # careful. need to validate some basic assumptions.
      unless global_options.force
        if manager.list_upward_links(from_shard_id).map { |link| link.up_id }.to_a != [ replica_shard_id ]
          STDERR.puts "Uplink from #{from_shard_id} is not a migration replica."
          exit 1
        end
        if manager.list_upward_links(to_shard_id).map { |link| link.up_id }.to_a != [ write_only_shard_id ]
          STDERR.puts "Uplink from #{to_shard_id} is not a write-only barrier."
          exit 1
        end
        if manager.list_upward_links(write_only_shard_id).map { |link| link.up_id }.to_a != [ replica_shard_id ]
          STDERR.puts "Uplink from write-only barrier is not a migration replica."
          exit 1
        end
      end

      manager.remove_link(write_only_shard_id, to_shard_id)
      manager.list_upward_links(replica_shard_id).each do |link|
        manager.remove_link(link.up_id, link.down_id)
        manager.add_link(link.up_id, to_shard_id, link.weight)
      end
      manager.replace_forwarding(replica_shard_id, to_shard_id)
      manager.delete_shard(replica_shard_id)
      manager.delete_shard(write_only_shard_id)
    end
  end

  class InjectCommand < Command
    def run
      count     = 0
      page_size = 20
      priority, *jobs = @argv
      help!("Requires priority") unless priority and jobs.size > 0

      jobs.each_slice(page_size) do |js|
        job_injector.inject_jobs(js.map {|j| Job.new(priority.to_i, j) })

        count += 1
        # FIXME add -q --quiet option
        STDERR.print "."
        STDERR.print "#{count * page_size}" if count % 10 == 0
        STDERR.flush
      end
      STDERR.print "\n"
    end
  end

  class FlushCommand < Command
    def run
      args = @argv[0]
      help!("Requires --all, or a job priority id.") unless args || command_options.flush_all
      if command_options.flush_all
        manager.retry_errors()
      else
        manager.retry_errors_for(args.to_i)
      end
    end
  end


  class AddHostCommand < Command
    def run
      hosts = @argv.map do |arg|
        cluster, hostname, port = *arg.split(":")
        help!("malformed host argument") unless [cluster, hostname, port].compact.length == 3

        Host.new(hostname, port.to_i, cluster, HostStatus::Normal)
      end

      hosts.each {|h| manager.add_remote_host(h) }
    end
  end

  class RemoveHostCommand < Command
    def run
      host = @argv[0].split(":")
      host.unshift nil if host.length == 2
      cluster, hostname, port = *host

      manager.remove_remote_host(hostname, port.to_i)
    end
  end

  class ListHostsCommand < Command
    def run
      manager.list_remote_hosts.each do |host|
        puts "#{[host.cluster, host.hostname, host.port].join(":")} #{host.status}"
      end
    end
  end

  class TablesCommand < Command
    def run
      puts manager.list_tables.join(" ")
    end
  end

  class TopologyCommand < Command
    def run
      manifest  = manager.manifest(*global_options.tables)
      templates = manifest.templates.inject({}) do |h, (t, fs)|
        h.update t.to_config => fs
      end

      if command_options.forwardings
        templates.
          inject([]) { |h, (t, fs)| fs.each { |f| h << [f.inspect, t] }; h }.
          sort.
          each { |a| puts "%s\t%s" % a }
      elsif command_options.root_shards
        templates.
          inject([]) { |a, (t, fs)| fs.each { |f| a << [f.shard_id.inspect, t] }; a }.
          sort.
          each { |a| puts "%s\t%s" % a }
      else
        templates.
          map { |(t, fs)| [fs.length, t] }.
          sort.reverse.
          each { |a| puts "%4d %s" % a }
      end
    end
  end

  class TransformTreeCommand < Command
    def run
      help!("wrong number of arguments") unless @argv.length == 2

      scheduler_options = command_options.scheduler_options || {}
      template_s, shard_id_s = @argv

      to_template    = ShardTemplate.parse(template_s)
      shard_id       = ShardId.parse(shard_id_s)
      base_name      = shard_id.table_prefix.split('_').first
      forwarding     = manager.get_forwarding_for_shard(shard_id)
      manifest       = manager.manifest(forwarding.table_id)
      shard          = manifest.trees[forwarding]
      copy_wrapper   = scheduler_options[:copy_wrapper]
      be_quiet       = global_options.force && command_options.quiet
      transformation = Transformation.new(shard.template, to_template, copy_wrapper)

      scheduler_options[:quiet] = be_quiet

      if transformation.noop?
        puts "Nothing to do!"
        exit
      end

      unless be_quiet
        puts transformation.inspect
        puts ""
      end

      unless global_options.force
        print "Continue? (y/n) "; $stdout.flush
        exit unless $stdin.gets.chomp == "y"
        puts ""
      end

      Gizzard.schedule! manager,
                        base_name,
                        { transformation => { forwarding => shard } },
                        scheduler_options
    end
  end

  class TransformCommand < Command
    def run
      help!("must have an even number of arguments") unless @argv.length % 2 == 0

      scheduler_options = command_options.scheduler_options || {}
      manifest          = manager.manifest(*global_options.tables)
      copy_wrapper      = scheduler_options[:copy_wrapper]
      be_quiet          = global_options.force && command_options.quiet
      transformations   = {}

      scheduler_options[:quiet] = be_quiet

      @argv.each_slice(2) do |(from_template_s, to_template_s)|
        from, to       = [from_template_s, to_template_s].map {|s| ShardTemplate.parse(s) }
        transformation = Transformation.new(from, to, copy_wrapper)
        forwardings    = Set.new(manifest.templates[from] || [])
        trees          = manifest.trees.reject {|(f, s)| !forwardings.include?(f) }

        transformations[transformation] = trees
      end

      transformations.reject! {|t,_| t.noop? }

      if transformations.empty?
        puts "Nothing to do!"
        exit
      end

      base_name = transformations.values.find {|v| !v.empty? }.values.find {|v| !v.nil?}.id.table_prefix.split('_').first

      unless be_quiet
        transformations.sort.each do |transformation, trees|
          puts transformation.inspect
          puts "Applied to #{trees.length} shards:"
          trees.keys.sort.each {|f| puts "  #{f.inspect}" }
        end
        puts ""
      end

      unless global_options.force
        print "Continue? (y/n) "; $stdout.flush
        exit unless $stdin.gets.chomp == "y"
        puts ""
      end

      Gizzard.schedule! manager,
                        base_name,
                        transformations,
                        scheduler_options
    end
  end

  class RebalanceCommand < Command
    def run
      help!("must have an even number of arguments") unless @argv.length % 2 == 0

      scheduler_options = command_options.scheduler_options || {}
      manifest          = manager.manifest(*global_options.tables)
      copy_wrapper      = scheduler_options[:copy_wrapper]
      be_quiet          = global_options.force && command_options.quiet
      shard_weights_file= command_options.shard_weights
      strategy          = command_options.strategy || "minimal"
      tolerance         = (command_options.tolerance || "0.05").to_f
      transformations   = {}

      scheduler_options[:quiet] = be_quiet

      dest_templates_and_weights = {}

      @argv.each_slice(2) do |(weight_s, to_template_s)|
        to     = ShardTemplate.parse(to_template_s)
        weight = weight_s.to_i

        dest_templates_and_weights[to] = weight
      end

      transformations = global_options.tables.inject({}) do |all, table|
        trees      = manifest.trees.reject {|(f, s)| f.table_id != table }
        rebalancer = Rebalancer.new(trees, dest_templates_and_weights, shard_weights_file, strategy, tolerance, copy_wrapper)

        all.update(rebalancer.transformations) {|t,a,b| a.merge b }
      end

      if transformations.empty?
        puts "Nothing to do!"
        exit
      end

      base_name = transformations.values.first.values.first.id.table_prefix.split('_').first

      unless be_quiet
        transformations.each do |transformation, trees|
          puts transformation.inspect
          puts "Applied to #{trees.length} shards:"
          trees.keys.sort.each {|f| puts "  #{f.inspect}" }
        end
        puts ""
      end

      unless global_options.force
        print "Continue? (y/n) "; $stdout.flush
        exit unless $stdin.gets.chomp == "y"
        puts ""
      end

      Gizzard.schedule! manager,
                        base_name,
                        transformations,
                        scheduler_options
    end
  end
end
