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

    def apply!(nameserver)
      @forwardings.each do |(base_id, shard_id)|
        forwarding = Forwarding.new(table_id, base_id, shard_id)

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
    module Op
      class BaseOp
        def apply_batch(nameserver, base_name, batch)
          # this level of indirection is so copy can loop through
          # items twice
          each_batch_item(nameserver, base_name, batch) do |table_id, base_id, table_prefix, translations|
            apply(nameserver, table_id, base_id, table_prefix, translations)
          end
        end

        def inverse?(other)
          Transformation::JOB_INVERSES[self.class] == other.class
        end

        def eql?(other)
          self.class == other.class
        end

        alias == eql?

        def inspect
          templates = (is_a?(LinkOp) ? [from, to] : [template]).map {|t| t.identifier }.join(" -> ")
          name      = Transformation::JOB_NAMES[self.class]
          "#{name}(#{templates})"
        end

        def <=>(other)
          JOB_PRIORITIES[self.class] <=> JOB_PRIORITIES[other.class]
        end

        private

        def each_batch_item(nameserver, base_name, batch)
          batch.each do |forwarding, shard|
            table_id     = forwarding.table_id
            base_id      = forwarding.base_id
            enum         = shard.enumeration
            table_prefix = Shard.canonical_table_prefix(enum, table_id, base_name)
            translations = shard.canonical_shard_id_map(base_name, table_id, enum)

            yield(table_id, base_id, table_prefix, translations)
          end
        end
      end

      class CopyShard < BaseOp
        BUSY = 1

        attr_reader :from, :to
        alias template to

        def initialize(from, to)
          @from = from
          @to   = to
        end

        def expand(*args); { :copy => [self] } end

        def apply_batch(nameserver, base_name, batch)
          each_batch_item(nameserver, base_name, batch) do |table_id, base_id, table_prefix, translations|
            from_shard_id = from.to_shard_id(table_prefix, translations)
            to_shard_id   = to.to_shard_id(table_prefix, translations)

            nameserver.mark_shard_busy(to_shard_id, BUSY)
            nameserver.copy_shard(from_shard_id, to_shard_id)
          end

          return if nameserver.dryrun

          each_batch_item(nameserver, base_name, batch) do |table_id, base_id, table_prefix, translations|
            sleep 5 while nameserver.get_shard(to.to_shard_id(table_prefix)).busy?
          end
        end
      end

      class LinkOp < BaseOp
        attr_reader :from, :to
        alias template to

        def initialize(from, to)
          @from = from
          @to   = to
        end

        def inverse?(other)
          super && self.from.link_eql?(other.from) && self.to.link_eql?(other.to)
        end

        def eql?(other)
          super && self.from.link_eql?(other.from) && self.to.link_eql?(other.to)
        end
      end

      class AddLink < LinkOp
        def expand(copy_source, involved_in_copy, wrapper_type)
          if involved_in_copy
            wrapper = ShardTemplate.new(wrapper_type, to.host, to.weight, '', '', [to])
            { :prepare => [AddLink.new(from, wrapper)],
              :cleanup => [self, RemoveLink.new(from, wrapper)] }
          else
            { :prepare => [self] }
          end
        end

        def apply(nameserver, table_id, base_id, table_prefix, translations)
          from_shard_id = from.to_shard_id(table_prefix, translations)
          to_shard_id   = to.to_shard_id(table_prefix, translations)

          nameserver.add_link(from_shard_id, to_shard_id, to.weight)
        end
      end

      class RemoveLink < LinkOp
        def expand(copy_source, involved_in_copy, wrapper_type)
          { (involved_in_copy ? :cleanup : :prepare) => [self] }
        end

        def apply(nameserver, table_id, base_id, table_prefix, translations)
          from_shard_id = from.to_shard_id(table_prefix, translations)
          to_shard_id   = to.to_shard_id(table_prefix, translations)

          nameserver.remove_link(from_shard_id, to_shard_id)
        end
      end

      class ShardOp < BaseOp
        attr_reader :template

        def initialize(template)
          @template = template
        end

        def inverse?(other)
          super && self.template.shard_eql?(other.template)
        end

        def eql?(other)
          super && self.template.shard_eql?(other.template)
        end
      end

      class CreateShard < ShardOp
        def expand(copy_source, involved_in_copy, wrapper_type)
          if involved_in_copy
            wrapper = ShardTemplate.new(wrapper_type, template.host, template.weight, '', '', [template])
            { :prepare => [self, CreateShard.new(wrapper), AddLink.new(wrapper, template)],
              :cleanup => [RemoveLink.new(wrapper, template), DeleteShard.new(wrapper)],
              :copy => [CopyShard.new(copy_source, template)] }
          else
            { :prepare => [self] }
          end
        end

        def apply(nameserver, table_id, base_id, table_prefix, translations)
          nameserver.create_shard(template.to_shard_info(table_prefix, translations))
        end
      end

      class DeleteShard < ShardOp
        def expand(copy_source, involved_in_copy, wrapper_type)
          { (involved_in_copy ? :cleanup : :prepare) => [self] }
        end

        def apply(nameserver, table_id, base_id, table_prefix, translations)
          nameserver.delete_shard(template.to_shard_id(table_prefix, translations))
        end
      end

      class SetForwarding < ShardOp
        def expand(copy_source, involved_in_copy, wrapper_type)
          if involved_in_copy
            wrapper = ShardTemplate.new(wrapper_type, nil, 0, '', '', [to])
            { :prepare => [SetForwarding.new(template, wrapper)],
              :cleanup => [self] }
          else
            { :prepare => [self] }
          end
        end

        def apply(nameserver, table_id, base_id, table_prefix, translations)
          shard_id   = template.to_shard_id(table_prefix, translations)
          forwarding = Forwarding.new(table_id, base_id, shard_id)
          nameserver.set_forwarding(forwarding)
        end
      end


      # XXX: A no-op, but needed for setup/teardown symmetry

      class RemoveForwarding < ShardOp
        def expand(copy_source, involved_in_copy, wrapper_type)
          { (involved_in_copy ? :cleanup : :prepare) => [self] }
        end

        def apply(nameserver, table_id, base_id, table_prefix, translations)
          # shard_id   = template.to_shard_id(table_prefix, translations)
          # forwarding = Forwarding.new(table_id, base_id, shard_id)
          # nameserver.remove_forwarding(forwarding)
        end
      end
    end

    JOB_NAMES = {
      Op::RemoveForwarding => "remove_forwarding",
      Op::RemoveLink       => "remove_link",
      Op::DeleteShard      => "delete_shard",
      Op::CreateShard      => "create_shard",
      Op::AddLink          => "add_link",
      Op::SetForwarding    => "set_forwarding",
      Op::CopyShard        => "copy_shard"
    }

    JOB_INVERSES = {
      Op::AddLink       => Op::RemoveLink,
      Op::CreateShard   => Op::DeleteShard,
      Op::SetForwarding => Op::RemoveForwarding
    }

    JOB_INVERSES.keys.each {|k| v = JOB_INVERSES[k]; JOB_INVERSES[v] = k }

    JOB_PRIORITIES = {
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

    def apply!(nameserver, base_name, batch)
      raise ArgumentError unless batch.is_a? Hash

      applier = lambda {|j| j.apply_batch(nameserver, base_name, batch) }

      operations[:prepare].each(&applier)
      operations[:copy].each(&applier)
      operations[:cleanup].each(&applier)
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
end
