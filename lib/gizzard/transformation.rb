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
      Op::DiffShards       => "diff_shards",
      Op::CommitBegin      => "commit_begin",
      Op::CommitEnd        => "commit_end"
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
      Op::CommitBegin      => 4,
      Op::RemoveForwarding => 5,
      Op::RemoveLink       => 6,
      Op::DeleteShard      => 7,
      Op::CopyShard        => 8,
      Op::RepairShards     => 9,
      Op::DiffShards       => 10,
      Op::CommitEnd        => 10000
    }

    ORDERED_OP_PHASES = [
      [:prepare, "PREPARE"],
      [:copy, "COPY"],
      [:repair, "REPAIR"],
      [:diff, "DIFF"],
      [:unblock_writes, "UNBLOCK_WRITES"],
      [:unblock_reads, "UNBLOCK_READS"],
      [:cleanup, "CLEANUP"],
    ]
    OP_PHASES = Hash[ORDERED_OP_PHASES]
    OP_PHASES_BY_NAME = OP_PHASES.invert

    DEFAULT_DEST_WRAPPER = 'BlockedShard'

    attr_reader :from, :to, :copy_dest_wrapper, :skip_copies

    # TODO: the skip_copies parameter should move out into the code that executes transforms
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
      o.is_a?(self.class) && (self <=> o) == 0
    end

    def <=>(o)
      if ((cmp = self.from <=> o.from) != 0); return cmp end
      if ((cmp = self.to <=> o.to) != 0); return cmp end
      self.copy_dest_wrapper <=> o.copy_dest_wrapper
    end

    def hash
      return @hash if @hash
      @hash = from.hash + to.hash + copy_dest_wrapper.hash
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

      # display phase lists in a particular order
      op_inspect = ORDERED_OP_PHASES.map do |phase_tuple|
        phase_name = phase_tuple.last
        inspect_phase = op_inspect[phase_tuple.first]
        inspect_phase.empty? ? "" : "  #{phase_name}\n#{inspect_phase}\n"
      end.join

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
      collapsed = jobs.reject do |job1|
        jobs.find do |job2|
          job1.inverse? job2
        end
      end
      # if all non-noop jobs have collapsed, entire transform was a noop
      collapsed.all?{|job| job.noop? } ? [] : collapsed
    end

    def expand_jobs(jobs)
      expanded = jobs.inject(initialize_op_phases) do |ops, job|
        job_ops = job.expand(self.copy_source, involved_in_copy?(job.template), @batch_finish)
        ops.update(job_ops) {|k,a,b| a + b }
      end

      # if there are no copies that need to take place, we can do all
      # nameserver changes in one step
      # TODO: unnecessary optimization, considering that all ops are roundtrips anyway
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
      jobs.concat [Op::CreateShard.new(root, @copy_dest_wrapper), Op::SetForwarding.new(root, @copy_dest_wrapper)]
    end

    def destroy_tree(root)
      # reminder: order doesn't matter here, since ops are sorted by priority
      jobs = visit_collect(root) do |parent, child|
        [Op::RemoveLink.new(parent, child), Op::DeleteShard.new(child)]
      end
      jobs.concat [Op::RemoveForwarding.new(root), Op::DeleteShard.new(root)]
      jobs.concat [Op::CommitBegin.new(root), Op::CommitEnd.new(root)]
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

    def apply!(nameserver, phase, rollback_log)
      transformation.operations[phase].each do |op|
        # execute the operation, and log the inverse
        transform_operation =
          op.apply(nameserver, @table_id, @base_id, @table_prefix, @translations)
        puts "pushing #{transform_operation}"
        rollback_log.push!(transform_operation) if rollback_log && transform_operation
      end
    end

    # true if any of the given phases contain operations
    def required?(*phases)
      phases.any?{|phase| !transformation.operations[phase].empty?}
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
  end
end
