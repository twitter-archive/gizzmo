require "set"

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
      Op::CopyShard        => "copy_shard",
      Op::RepairShards     => "repair_shards",
      Op::DiffShards       => "diff_shards"
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
      Op::CopyShard        => 7,
      Op::RepairShards     => 8,
      Op::DiffShards       => 9
    }

    OP_PHASES = {
      :prepare => "PREPARE",
      :copy => "COPY",
      :repair => "REPAIR",
      :unblock_writes => "UNBLOCK_WRITES",
      :unblock_reads => "UNBLOCK_READS",
      :cleanup => "CLEANUP",
      :diff => "DIFF"
    }

    DEFAULT_DEST_WRAPPER = 'BlockedShard'

    attr_reader :from, :to, :copy_dest_wrapper, :skip_copies

    def initialize(from_template, to_template, copy_dest_wrapper = nil, skip_copies = false, batch_finish = false)
      copy_dest_wrapper ||= DEFAULT_DEST_WRAPPER

      unless Shard::VIRTUAL_SHARD_TYPES.include? copy_dest_wrapper
        raise ArgumentError, "#{copy_dest_wrapper} is not a valid virtual shard type."
      end

      @from = from_template
      @to   = to_template
      @copy_dest_wrapper = copy_dest_wrapper
      @skip_copies = skip_copies
      @batch_finish = batch_finish

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

    def noop?
      from.eql? to
    end

    def eql?(o)
      o.is_a?(self.class) &&
      from.eql?(o.from) &&
      to.eql?(o.to) &&
      copy_dest_wrapper.eql?(o.copy_dest_wrapper)
    end

    def <=>(o)
      to_a = lambda {|t| [t.from, t.to, t.copy_dest_wrapper] }

      to_a.call(self) <=> to_a.call(o)
    end

    def hash
      from.hash + to.hash + copy_dest_wrapper.hash
    end

    # create a map of empty phase lists
    def initialize_op_phases
      Hash[OP_PHASES.keys.map do |phase| [phase, []] end]
    end

    def inspect
      # TODO: Need to limit this to e.g. 10 ops in the list, and show a total
      # count instead of showing the whole thing.
      op_inspect = operations.inject({}) do |h, (phase, ops)|
        h.update phase => ops.map {|job| "    #{job.inspect}" }.join("\n")
      end

      # TODO: This seems kind of daft to copy around these long strings.
      # Loop over it once just for display?
      phase_line = lambda do |phase|
        op_inspect[phase].empty? ? "" : "  #{OP_PHASES[phase]}\n#{op_inspect[phase]}\n"
      end

      # display phase lists in a particular order
      op_inspect = [
        phase_line.call(:prepare),
        phase_line.call(:copy),
        phase_line.call(:repair),
        phase_line.call(:diff),
        phase_line.call(:unblock_writes),
        phase_line.call(:unblock_reads),
        phase_line.call(:cleanup)
      ].join

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
      expanded = jobs.inject(initialize_op_phases) do |ops, job|
        job_ops = job.expand(self.copy_source, involved_in_copy?(job.template), @batch_finish)
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
      return false if skip_copies
      return @copies_required unless  @copies_required.nil?

      @copies_required = !from.nil? &&
        to.concrete_descendants.select {|d| !from.shared_host? d }.length > 0
    end

    def involved_in_copy?(template)
      in_copied_subtree?(template) || copy_destination?(template)
    end

    def copy_destination?(template)
      copies_required? && template.concrete? && !from.shared_host?(template)
    end

    def in_copied_subtree?(template)
      copies_required? && !!from.descendants.find {|s| s.shard_eql? template }
    end

    def copy_source
      from.copy_sources.first if copies_required?
    end

    def create_tree(root)
      get_wrapper_type = Proc.new { |template, wrapper_type|
        if wrapper_type.nil?
          nil
        elsif template.contains_shard_type? wrapper_type 
          nil
        else
          wrapper_type
        end
      }
      jobs = visit_collect(root, get_wrapper_type, @copy_dest_wrapper) do |parent, child, wrapper|
        [Op::CreateShard.new(child, wrapper), Op::AddLink.new(parent, child, wrapper)]
      end
      [Op::CreateShard.new(root, @copy_dest_wrapper)].concat jobs << Op::SetForwarding.new(root, @copy_dest_wrapper)
    end

    def destroy_tree(root)
      jobs = visit_collect(root) do |parent, child|
        [Op::RemoveLink.new(parent, child), Op::DeleteShard.new(child)]
      end
      [Op::RemoveForwarding.new(root)].concat jobs << Op::DeleteShard.new(root)
    end

    private

    def visit_collect(parent, pass_down_method=Proc.new{}, pass_down_value=nil, &block)
      parent.children.inject([]) do |acc, child|
        pass_down_value = pass_down_method.call(child, pass_down_value)
        visit_collect(child, pass_down_method, pass_down_value, &block).concat(acc.concat(block.call(parent, child, pass_down_value)))
      end
    end
  end

  class BoundTransformation
    attr_reader :transformation, :base_name, :forwarding, :shard

    def from; transformation.from end
    def to;   transformation.to   end

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

    # TODO: replace with single execute(:nameserver, :phase) method?

    def prepare!(nameserver)
      apply_ops(nameserver, transformation.operations[:prepare])
    end

    def copy_required?
      !transformation.operations[:copy].empty?
    end

    def copy!(nameserver)
      apply_ops(nameserver, transformation.operations[:copy])
    end

    def unblock_required?
      !transformation.operations[:unblock_writes].empty? || !transformation.operations[:unblock_reads].empty?
    end

    def unblock_writes!(nameserver)
      apply_ops(nameserver, transformation.operations[:unblock_writes])
    end

    def unblock_reads!(nameserver)
      apply_ops(nameserver, transformation.operations[:unblock_reads])
    end

    def cleanup!(nameserver)
      apply_ops(nameserver, transformation.operations[:cleanup])
    end

    def involved_shards(phase = :copy)
      @involved_shards        ||= {}
      @involved_shards[phase] ||=
        Set.new(transformation.operations[phase].map do |op|
          op.involved_shards(@table_prefix, @translations)
        end.flatten.compact.uniq)
    end

    def involved_hosts_array(phase = :copy)
      @involved_hosts_array        ||= {}
      @involved_hosts_array[phase] ||= involved_shards(phase).map {|s| s.hostname }.uniq
    end

    def involved_hosts(phase = :copy)
      @involved_hosts        ||= {}
      @involved_hosts[phase] ||= Set.new(involved_hosts_array(phase))
    end

    def inspect
      "#{@forwarding.inspect}: #{from.inspect} => #{to.inspect}"
    end

    def copy_descs
      transformation.operations[:copy].map do |copy|
        desc = copy.shards.inject("") do |d, shard|
          d += shard.to_shard_id(@table_prefix, @translations).inspect + " <-> "
        end
        desc.chomp " <-> "
      end
      
    end

    private

    def apply_ops(nameserver, ops)
      ops.each do |op|
        op.apply(nameserver, @table_id, @base_id, @table_prefix, @translations)
      end
    end
  end
end
