module Gizzard
  class ForwardingTransformation
    def initialize(table_id, forwardings)
      @table_id = table_id
      @forwardings = forwardings.dup
    end

    def paginate(page_size = 1)
      [self]
    end

    def must_copy?
      false
    end

    def apply!(nameserver, config)
      @forwardings.each do |(base_id, table)|
        shard_id = ShardTemplate.new("com.twitter.gizzard.shards.ReplicatingShard", "localhost", 0, '', '', []).to_shard_id(table)
        forwarding = Thrift::Forwarding.new(table_id, base_id, shard_id)

        nameserver.set_forwarding(forwarding)
      end
    end

    def inspect(with_shards = false)
      forwardings_inspect = @forwardings.sort_by { |base, table| base }.map do |base, table|
        "  #{'%d %016x' % [table_id, base]} -> #{table}"
      end.join("\n")

      "[#{@forwardings.length} FORWARDINGS:\n#{forwardings_inspect}\n]"
    end

    def table_id
      @table_id || 0
    end
  end

  class Transformation
    attr_reader :from, :to, :shards

    DEFAULT_CONCURRENT_COPIES = 5

    JOB_INVERSES = {
      :add_link => :remove_link,
      :remove_link => :add_link,
      :create_shard => :delete_shard,
      :delete_shard => :create_shard
    }

    JOB_PRIORITIES = {
      :remove_link => 0,
      :delete_shard =>1,
      :create_shard => 2,
      :add_link => 3,
      :copy_shard => 4
    }

    def initialize(from_template, to_template, shards)
      @from = from_template
      @to = to_template
      @shards = shards
    end

    def paginate(page_size = DEFAULT_CONCURRENT_COPIES)
      if must_copy?
        slices = shards.inject([[]]) do |slices, id|
          slices.last << id
          slices << [] if slices.last.length >= page_size
          slices
        end

        slices.inject([]) do |pages,slice|
          pages << self.class.new(from, to, slice) unless slice.empty?
          pages
        end
      else
        [self]
      end
    end

    def apply!(nameserver, config)
      raise "involves copies!" if must_copy?

      prepare! nameserver, config
      cleanup! nameserver, config
    end

    def prepare!(nameserver, config)
      operations[:prepare].each {|job| apply_job(job, nameserver, config) }
    end

    def copy!(nameserver, config)
      operations[:copy].each {|job| apply_job(job, nameserver, config) }
    end

    def must_copy?
      !operations[:copy].empty?
    end

    def wait_for_copies(nameserver, config)
      return if nameserver.dryrun?

      operations[:copy].each do |(type, from, to)|
        each_shard(config) do
          if nameserver.get_shard(id(to)).busy?
            sleep 1; redo
          end
        end
      end
    end

    def cleanup!(nameserver, config)
      operations[:cleanup].each {|job| apply_job(job, nameserver, config) }
    end

    def inspect(with_shards = false)
      op_inspect = operations.inject({}) do |h, (phase, ops)|
        h[phase] = ops.map do |(type, arg1, arg2)|
          arg2id = arg2 ? ", #{arg2.identifier}" : ""
          "    #{type}, #{arg1.identifier}#{arg2id}"
        end.join("\n")
        h
      end

      prepare_inspect = op_inspect[:prepare].empty? ? "" : "  PREPARE\n#{op_inspect[:prepare]}\n"
      copy_inspect = op_inspect[:copy].empty? ? "" : "  COPY\n#{op_inspect[:copy]}\n"
      cleanup_inspect = op_inspect[:cleanup].empty? ? "" : "  CLEANUP\n#{op_inspect[:cleanup]}\n"

      op_inspect = [prepare_inspect, copy_inspect, cleanup_inspect].join

      if with_shards
        "[#{shards.length} SHARDS: #{shards.sort.map {|s| "%04d" % s }.join(', ') }\n\n #{from.inspect} => #{to.inspect} : \n#{op_inspect}\n]"
      else
        "[#{shards.length} SHARDS: #{from.inspect} => #{to.inspect} : \n#{op_inspect}\n]"
      end
    end



    def operations
      return @operations if @operations

      log = []
      log.concat destroy_tree(from) if from
      log.concat create_tree(to) if to

      # compact
      log = collapse_jobs(log)

      @operations = log.inject({:prepare => [], :copy => [], :cleanup => []}) do |ops, job|
        op =
          case job.first # job type
          when :add_link, :create_shard
            expand_create_job(job)
          when :remove_link, :delete_shard
            expand_delete_job(job)
          else
            raise "Unknown job type, cannot expand"
          end

        ops.update(op) {|k,a,b| a.concat(b) }
      end

      # if there are no copies that need to take place, we can do all
      # nameserver changes in one step
      if @operations[:copy].empty?
        @operations[:prepare].concat @operations[:cleanup]
        @operations[:cleanup] = []
      end

      @operations.each do |(phase, jobs)|
        jobs.replace(sort_jobs(jobs))
      end

      @operations
    end

    def collapse_jobs(jobs)
      jobs.reject do |(type_1, arg1_1, arg2_1)|
        jobs.find do |(type_2, arg1_2, arg2_2)|
          if JOB_INVERSES[type_1] == type_2
            if arg2_1.nil? # shard creation. wieght doesn't matter.
              arg1_1.eql?(arg1_2, false, false)
            else
              arg1_1.eql?(arg1_2, false, false) && arg2_1.eql?(arg2_2, false)
            end
          else
            false
          end
        end
      end
    end

    def expand_create_job(job)
      type, arg1, arg2 = job
      template = (type == :create_shard) ? arg1 : arg2

      ops = {:prepare => [], :copy => [], :cleanup => []}

      if copy_destination? template
        write_only_wrapper = ShardTemplate.new('WriteOnlyShard', nil, 0, '', '', [template])

        if type == :add_link
          ops[:prepare] << add_link(arg1, write_only_wrapper)
          ops[:cleanup] << job
          ops[:cleanup] << remove_link(arg1, write_only_wrapper)
        else
          ops[:prepare] << job
          ops[:prepare] << create_shard(write_only_wrapper)
          ops[:prepare] << add_link(write_only_wrapper, arg1)

          ops[:cleanup] << remove_link(write_only_wrapper, arg1)
          ops[:cleanup] << delete_shard(write_only_wrapper)

          ops[:copy] << copy_shard(copy_source, arg1)
        end
      else
        ops[:prepare] << job
      end

      ops
    end

    def expand_delete_job(job)
      type, arg1, arg2 = job
      template = (type == :delete_shard) ? arg1 : arg2

      ops = {:prepare => [], :copy => [], :cleanup => []}

      if copy_source? template
        ops[:cleanup] << job
      else
        ops[:prepare] << job
      end

      ops
    end

    def sort_jobs(jobs)
      jobs.sort_by {|(type, _, _)| JOB_PRIORITIES[type] }
    end

    def apply_job(job, nameserver, config)
      type, arg1, arg2 = job

      case type
      when :copy_shard
        each_shard(config) { nameserver.copy_shard(id(arg1), id(arg2)) }
      when :add_link
        each_shard(config) { nameserver.add_link(id(arg1), id(arg2), arg2.weight) }
      when :remove_link
        each_shard(config) { nameserver.remove_link(id(arg1), id(arg2)) }
      when :create_shard
        each_shard(config) { nameserver.create_shard(info(arg1)) }
      when :delete_shard
        each_shard(config) { nameserver.delete_shard(id(arg1)) }
      else
        raise ArgumentError, "unknown job type #{type.inspect}"
      end
    end

    def each_shard(config)
      @current_config = config
      shards.each do |shard|
        @current_shard = shard
        yield
      end
    ensure
      @current_config = @current_shard = nil
    end

    def id(template)
      @current_shard or raise "no current shard id!"
      name = @current_config.shard_name(@current_shard)

      canonical = template.to_shard_id(name)
      @current_config.manifest.existing_shard_ids[canonical] || canonical
    end

    def info(template)
      id = id(template)

      info = template.to_shard_info(id.table_prefix)
      info.id = id(template)
      info
    end

    def copy_destination?(template)
      template.concrete? && !from.nil? && !from.descendant_identifiers.include?(template.identifier)
    end


    def copy_source
      from.copy_source if from
    end

    def copy_source?(template)
      return false unless copy_source
      template.descendant_identifiers.include? copy_source.identifier
    end

    def add_link(from, to)
      [:add_link, from, to]
    end

    def remove_link(from, to)
      [:remove_link, from, to]
    end

    def create_shard(template)
      [:create_shard, template]
    end

    def delete_shard(template)
      [:delete_shard, template]
    end

    def copy_shard(from, to)
      [:copy_shard, from, to]
    end

    def create_tree(root)
      log = []

      log << create_shard(root)
      root.children.each do |child|
        log.concat create_tree(child)
        log << add_link(root, child)
      end

      log
    end

    def destroy_tree(root)
      log = []

      root.children.each do |child|
        log << remove_link(root, child)
        log.concat destroy_tree(child)
      end
      log << delete_shard(root)

      log
    end
  end
end
