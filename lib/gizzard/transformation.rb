module Gizzard
  class Transformation
    require 'gizzard/transformation_op'
    require 'gizzard/transformation_scheduler'

    OP_NAMES = {
      Op::RemoveForwarding => "remove_forwarding",
      Op::RemoveLink       => "remove_link",
      Op::DeleteShard      => "delete_shard",
      Op::CreateShard      => "create_shard",
      Op::AddLink          => "add_link",
      Op::SetForwarding    => "set_forwarding",
      Op::CopyShard        => "copy_shard"
    }

    OP_INVERSES = {
      Op::AddLink       => Op::RemoveLink,
      Op::CreateShard   => Op::DeleteShard,
      Op::SetForwarding => Op::RemoveForwarding
    }

    OP_INVERSES.keys.each {|k| v = OP_INVERSES[k]; OP_INVERSES[v] = k }

    OP_PRIORITIES = {
      Op::CreateShard      => 1,
      Op::AddLink          => 2,
      Op::SetForwarding    => 3,
      Op::RemoveForwarding => 4,
      Op::RemoveLink       => 5,
      Op::DeleteShard      => 6,
      Op::CopyShard        => 7
    }

    DEFAULT_DEST_WRAPPER = 'WriteOnlyShard'

    attr_reader :from, :to

    def initialize(from_template, to_template, copy_dest_wrapper = DEFAULT_DEST_WRAPPER)
      @from = from_template
      @to   = to_template
      @copy_dest_wrapper = copy_dest_wrapper

      if copies_required? && copy_source.nil?
        raise ArgumentError, "copy required without a valid copy source"
      end
    end

    def bind(base_name, forwardings_to_shards)
      raise ArgumentError unless forwardings_to_shards.is_a? Hash

      forwardings_to_shards.map do |forwarding, shard|
        BoundTransformation.new(self, base_name, forwarding, shard)
      end
    end

    def inspect
      op_inspect = operations.inject({}) do |h, (phase, ops)|
        h.update phase => ops.map {|job| "    #{job.inspect}" }.join("\n")
      end

      prepare_inspect = op_inspect[:prepare].empty? ? "" : "  PREPARE\n#{op_inspect[:prepare]}\n"
      copy_inspect    = op_inspect[:copy].empty?    ? "" : "  COPY\n#{op_inspect[:copy]}\n"
      cleanup_inspect = op_inspect[:cleanup].empty? ? "" : "  CLEANUP\n#{op_inspect[:cleanup]}\n"

      op_inspect = [prepare_inspect, copy_inspect, cleanup_inspect].join

      "#{from.inspect} => #{to.inspect} :\n#{op_inspect}"
    end

    def operations
      return @operations if @operations

      log = []
      log.concat destroy_tree(from) if from
      log.concat create_tree(to) if to

      # compact
      log = collapse_jobs(log)

      @operations = expand_jobs(log)

      @operations.each do |(phase, jobs)|
        jobs.sort!
      end

      @operations
    end

    def collapse_jobs(jobs)
      jobs.reject do |job1|
        jobs.find do |job2|
          job1.inverse? job2
        end
      end
    end

    def expand_jobs(jobs)
      expanded = jobs.inject({:prepare => [], :copy => [], :cleanup => []}) do |ops, job|
        job_ops = job.expand(self.copy_source, involved_in_copy?(job.template), @copy_dest_wrapper)
        ops.update(job_ops) {|k,a,b| a + b }
      end

      # if there are no copies that need to take place, we can do all
      # nameserver changes in one step
      if expanded[:copy].empty?
        expanded[:prepare].concat expanded[:cleanup]
        expanded[:cleanup] = []
      end

      expanded
    end

    def copies_required?
      return @copies_required unless  @copies_required.nil?
      @copies_required = !from.nil? &&
        to.concrete_descendants.reject {|d| from.shared_host? d }.length > 0
    end

    def involved_in_copy?(template)
      copy_source?(template) || copy_destination?(template)
    end

    def copy_destination?(template)
      copies_required? && template.concrete? && !from.shared_host?(template)
    end

    def copy_source?(template)
      copies_required? && !!from.copy_sources.find {|s| s.shard_eql? template }
    end

    def copy_source
      from.copy_sources.first if copies_required?
    end

    def create_tree(root)
      jobs = visit_collect(root) do |parent, child|
        [Op::CreateShard.new(child), Op::AddLink.new(parent, child)]
      end
      [Op::CreateShard.new(root)].concat jobs << Op::SetForwarding.new(root)
    end

    def destroy_tree(root)
      jobs = visit_collect(root) do |parent, child|
        [Op::RemoveLink.new(parent, child), Op::DeleteShard.new(child)]
      end
      [Op::RemoveForwarding.new(root)].concat jobs << Op::DeleteShard.new(root)
    end

    private

    def visit_collect(parent, &block)
      parent.children.inject([]) do |acc, child|
        visit_collect(child, &block).concat(acc.concat(block.call(parent, child)))
      end
    end
  end


  class BoundTransformation
    attr_reader :transformation, :base_name, :forwarding, :shard

    def initialize(transformation, base_name, forwarding, shard)
      @transformation = transformation
      @base_name      = base_name
      @forwarding     = forwarding
      @shard          = shard

      @table_id       = forwarding.table_id
      @base_id        = forwarding.base_id
      @enum           = shard.enumeration
      @table_prefix   = Shard.canonical_table_prefix(@enum, @table_id, base_name)
      @translations   = shard.canonical_shard_id_map(base_name, @table_id, @enum)
    end

    def prepare!(nameserver)
      apply_ops(nameserver, transformation.operations[:prepare])
    end

    def copy!(nameserver)
      apply_ops(nameserver, transformation.operations[:copy])
    end

    def cleanup!(nameserver)
      apply_ops(nameserver, transformation.operations[:cleanup])
    end

    def involved_shards(phase = :copy)
      transformation.operations[phase].map do |op|
        op.involved_shards(@table_prefix, @translations)
      end.flatten.compact.uniq
    end

    def involved_hosts(phase = :copy)
      involved_shards(phase).map {|s| s.hostname }.uniq
    end

    private

    def apply_ops(nameserver, ops)
      ops.each do |op|
        op.apply(nameserver, @table_id, @base_id, @table_prefix, @translations)
      end
    end
  end
end
