module Gizzard
  class ForwardingTransformation
    def initialize(forwardings)
      @forwardings = forwardings.dup
    end

    def paginate(page_size = 1)
      [self]
    end

    def must_copy?
      false
    end

    def apply!(nameserver)
      @forwardings.each do |(base_id, table)|
        shard_id = Thrift::ShardId.new('localhost', "#{table}_replicating")
        forwarding = Thrift::Forwarding.new(0, base_id, shard_id)

        nameserver.set_forwarding(forwarding)
      end
    end

    def inspect(with_shards = false)
      forwardings_inspect = @forwardings.map do |(base, table)|
        "  #{base.to_s(16)} -> #{table}"
      end.join("\n")

      "[#{@forwardings.length} FORWARDINGS:\n#{forwardings_inspect}\n]"
    end
  end

  class Transformation
    attr_reader :from, :to, :shard_ids

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

    def initialize(from_template, to_template, shard_ids, config)
      @from = from_template
      @to = to_template
      @shard_ids = shard_ids
      @config = config
    end

    def paginate(page_size = DEFAULT_CONCURRENT_COPIES)
      if must_copy?
        slices = shard_ids.inject([[]]) do |slices, id|
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

    def apply!(nameserver)
      raise "involves copies!" if must_copy?

      prepare! nameserver
      cleanup! nameserver
    end

    def prepare!(nameserver)
      operations[:prepare].each {|job| apply_job(job, nameserver) }
    end

    def copy!(nameserver)
      operations[:copy].each {|job| apply_job(job, nameserver) }
    end

    def must_copy?
      !operations[:copy].empty?
    end

    def wait_for_copies(nameserver)
      return if nameserver.dryrun?

      operations[:copy].each do |(type, from, to)|
        each_id do
          if nameserver.get_shard(id(to)).busy?
            sleep 1; redo
          end
        end
      end
    end

    def cleanup!(nameserver)
      operations[:cleanup].each {|job| apply_job(job, nameserver) }
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
        "[#{shard_ids.length} SHARDS: #{shard_ids.sort.join(', ') }\n\n #{from.inspect} => #{to.inspect} : \n#{op_inspect}\n]"
      else
        "[#{shard_ids.length} SHARDS: #{from.inspect} => #{to.inspect} : \n#{op_inspect}\n]"
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
          JOB_INVERSES[type_1] == type_2 &&
            arg1_1.eql?(arg1_2, false) &&
            (arg2_1.nil? || arg2_1.eql?(arg2_2, false))
        end
      end
    end

    def expand_create_job(job)
      type, arg1, arg2 = job
      shard = (type == :create_shard) ? arg1 : arg2

      ops = {:prepare => [], :copy => [], :cleanup => []}

      if copy_destination? shard
        write_only_wrapper = ShardTemplate.new(:write_only, nil, 0, [shard])

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
      shard = (type == :delete_shard) ? arg1 : arg2

      ops = {:prepare => [], :copy => [], :cleanup => []}

      if copy_source? shard
        ops[:cleanup] << job
      else
        ops[:prepare] << job
      end

      ops
    end

    def sort_jobs(jobs)
      jobs.sort_by {|(type, _, _)| JOB_PRIORITIES[type] }
    end

    def apply_job(job, nameserver)
      type, arg1, arg2 = job

      case type
      when :copy_shard
        each_id { nameserver.copy_shard(id(arg1), id(arg2)) }
      when :add_link
        each_id { nameserver.add_link(id(arg1), id(arg2), arg2.weight) }
      when :remove_link
        each_id { nameserver.remove_link(id(arg1), id(arg2)) }
      when :create_shard
        each_id { nameserver.create_shard(info(arg1)) }
      when :delete_shard
        each_id { nameserver.delete_shard(id(arg1)) }
      else
        raise ArgumentError, "unknown job type #{type.inspect}"
      end
    end

    def each_id
      shard_ids.each do |id|
        @current_shard_id = id
        yield
      end
    ensure
      @current_shard_id = nil
    end

    def id(shard)
      shard_id = @current_shard_id or raise "no current shard id!"
      shard.to_shard_id(shard_id)
    end

    def info(shard)
      shard_id = @current_shard_id or raise "no current shard id!"
      shard.to_shard_info(@config, shard_id)
    end

    def copy_destination?(shard)
      shard.concrete? && !from.nil? && !from.descendant_identifiers.include?(shard.identifier)
    end


    def copy_source
      from.copy_source if from
    end

    def copy_source?(shard)
      return false unless copy_source
      shard.descendant_identifiers.include? copy_source.identifier
    end

    def add_link(from, to)
      [:add_link, from, to]
    end

    def remove_link(from, to)
      [:remove_link, from, to]
    end

    def create_shard(shard)
      [:create_shard, shard]
    end

    def delete_shard(shard)
      [:delete_shard, shard]
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
