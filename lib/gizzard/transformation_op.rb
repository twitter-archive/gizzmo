module Gizzard
  module Transformation::Op
    class BaseOp
      def inverse?(other)
        Transformation::OP_INVERSES[self.class] == other.class
      end

      def eql?(other)
        self.class == other.class
      end

      alias == eql?

      def inspect
        templates = (is_a?(LinkOp) ? [from, to] : [*template]).map {|t| t.identifier }.join(" -> ")
        name      = Transformation::OP_NAMES[self.class]
        "#{name}(#{templates})"
      end

      def <=>(other)
        Transformation::OP_PRIORITIES[self.class] <=> Transformation::OP_PRIORITIES[other.class]
      end

      def involved_shards(table_prefix, translations)
        []
      end
    end

    class CopyShard < BaseOp
      BUSY = 1

      attr_reader :from, :to, :shards
      alias template shards

      def initialize(*shards)
        @shards = shards
      end

      def expand(*args); { :copy => [self] } end

      def involved_shards(table_prefix, translations)
        shards.map{|s| s.to_shard_id(table_prefix, translations)}
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        involved_shards(table_prefix, translations).each { |sid| nameserver.mark_shard_busy(sid, BUSY) }
        nameserver.copy_shard(involved_shards(table_prefix, translations))
      end
    end

    class RepairShards < BaseOp
      attr_reader :from, :to
      alias template to

      def initialize(*shards)
        @shards = shards
      end

      def expand(*args); { :repair => [self] } end

      def involved_shards(table_prefix, translations)
        shards.map{|s| s.to_shard_id(table_prefix, translations)}
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        nameserver.repair_shards(involved_shards(table_prefix, translations))
      end
    end

    class DiffShards < BaseOp
      attr_reader :from, :to
      alias template to

      def initialize(from, to)
        @from = from
        @to   = to
      end

      def expand(*args); { :repair => [self] } end

      def involved_shards(table_prefix, translations)
        [to.to_shard_id(table_prefix, translations)]
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        from_shard_id = from.to_shard_id(table_prefix, translations)
        to_shard_id   = to.to_shard_id(table_prefix, translations)

        nameserver.diff_shards(from_shard_id, to_shard_id)
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
end
