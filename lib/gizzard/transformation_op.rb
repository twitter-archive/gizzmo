module Gizzard
  # the 'apply' method of a Transformation::Op should return a Thrift TransformOperation
  # object, representing the INVERSE of the applied operation, or 'Nil' if the operation is
  # not intended to be rolled back
  # TODO: over time, the batch_execute method should be utilized more fully by the
  # scheduler to compose multiple operations where safe/possible
  module Transformation::Op
    class BaseOp
      def inverse?(other)
        # FIXME: move inverses onto ops themselves
        Transformation::OP_INVERSES[self.class] == other.class
      end

      def eql?(other)
        self.class == other.class
      end

      alias == eql?

      def inverse
        return nil
      end

      def inspect
        templates = (is_a?(LinkOp) ? [from, to] : [*template]).map {|t| t.identifier }.join(" -> ")
        name      = Transformation::OP_NAMES[self.class]
        "#{name}(#{templates})"
      end

      def <=>(other)
        self_class = Transformation::OP_PRIORITIES[self.class]
        other_class = Transformation::OP_PRIORITIES[other.class]
        if ((cmp = self_class <=> other_class) != 0); return cmp end
        # comparing the template is not strictly necessary, but gives us a stable sort
        self.template <=> other.template
      end

      def involved_shards(table_prefix, translations)
        []
      end

      def noop?
        false
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
        nil
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
        nil
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
        nil
      end
    end

    class LinkOp < BaseOp
      attr_reader :from, :to
      alias template to

      def initialize(from, to, wrapper_type=nil)
        @from = from
        @to   = to
        @wrapper_type = wrapper_type
      end

      def inverse?(other)
        super && self.from.link_eql?(other.from) && self.to.link_eql?(other.to)
      end

      def inverse
        inv_class = Transformation::OP_INVERSES[self.class]
        return nil if inv_class.nil?
        inv_class.new(@from, @to)
      end

      def eql?(other)
        super && self.from.link_eql?(other.from) && self.to.link_eql?(other.to)
      end
    end

    class AddLink < LinkOp
      def expand(copy_source, involved_in_copy, batch_finish)
        # TODO: enforce that wrapper definitions match everywhere
        if !batch_finish && involved_in_copy && @wrapper_type
          copy_wrapper = ShardTemplate.new(@wrapper_type, to.host, to.weight, '', '', [to])
          { :prepare => [AddLink.new(from, copy_wrapper)],
            :cleanup => [self, RemoveLink.new(from, copy_wrapper)] }
        elsif batch_finish && involved_in_copy && @wrapper_type
          copy_wrapper = ShardTemplate.new(@wrapper_type, to.host, to.weight, '', '', [to])
          unblock_write_wrapper = ShardTemplate.new('WriteOnlyShard', to.host, to.weight, '', '', [to])
          { :prepare => [AddLink.new(from, copy_wrapper)],
            :unblock_writes => [AddLink.new(from, unblock_write_wrapper), RemoveLink.new(from, copy_wrapper)],
            :unblock_reads => [self, RemoveLink.new(from, unblock_write_wrapper)] }
        else
          { :prepare => [self] }
        end
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        from_shard_id = from.to_shard_id(table_prefix, translations)
        to_shard_id   = to.to_shard_id(table_prefix, translations)

        nameserver.add_link(from_shard_id, to_shard_id, to.weight)
        # return inverse
        TransformOperation.with(:remove_link, RemoveLinkRequest.new(from_shard_id, to_shard_id))
      end
    end

    class RemoveLink < LinkOp
      def expand(copy_source, involved_in_copy, batch_finish)
        { (involved_in_copy ? :cleanup : :prepare) => [self] }
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        from_shard_id = from.to_shard_id(table_prefix, translations)
        to_shard_id   = to.to_shard_id(table_prefix, translations)

        nameserver.remove_link(from_shard_id, to_shard_id)
        # return inverse
        TransformOperation.with(:add_link, AddLinkRequest.new(from_shard_id, to_shard_id, to.weight))
      end
    end

    class ShardOp < BaseOp
      attr_reader :template

      def initialize(template, wrapper_type=nil)
        @template = template
        @wrapper_type = wrapper_type
      end

      def inverse?(other)
        super && self.template.shard_eql?(other.template)
      end

      def inverse
        inv_class = Transformation::OP_INVERSES[self.class]
        return nil if inv_class.nil?
        inv_class.new(@template, @wrapper_type)
      end

      def eql?(other)
        super && self.template.shard_eql?(other.template)
      end
    end

    class CreateShard < ShardOp
      def expand(copy_source, involved_in_copy, batch_finish)
        # TODO: enforce that wrapper definitions match everywhere
        if !batch_finish && involved_in_copy && @wrapper_type
          copy_wrapper = ShardTemplate.new(@wrapper_type, template.host, template.weight, '', '', [template])
          { :prepare => [self, CreateShard.new(copy_wrapper), AddLink.new(copy_wrapper, template)],
            :cleanup => [RemoveLink.new(copy_wrapper, template), DeleteShard.new(copy_wrapper)],
            :copy => [CopyShard.new(copy_source, template)] }
        elsif batch_finish && involved_in_copy && @wrapper_type
          copy_wrapper = ShardTemplate.new(@wrapper_type, template.host, template.weight, '', '', [template])
          unblock_write_wrapper = ShardTemplate.new('WriteOnlyShard', template.host, template.weight, '', '', [template])
          { :prepare => [self, CreateShard.new(copy_wrapper), AddLink.new(copy_wrapper, template)],
            :unblock_writes => [RemoveLink.new(copy_wrapper, template), DeleteShard.new(copy_wrapper),
              CreateShard.new(unblock_write_wrapper), AddLink.new(unblock_write_wrapper, template)],
            :unblock_reads => [RemoveLink.new(unblock_write_wrapper, template), DeleteShard.new(unblock_write_wrapper)],
            :copy => [CopyShard.new(copy_source, template)] }
        elsif involved_in_copy
          # TODO: when would a wrapper type not be defined? should this still be supported?
          { :prepare => [self],
            :copy => [CopyShard.new(copy_source, template)] }
        else
          { :prepare => [self] }
        end
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        shard_info = template.to_shard_info(table_prefix, translations)
        nameserver.create_shard(shard_info)
        # return inverse
        TransformOperation.with(:delete_shard, shard_info.id)
      end
    end

    class DeleteShard < ShardOp
      def expand(copy_source, involved_in_copy, batch_finish)
        { (involved_in_copy ? :cleanup : :prepare) => [self] }
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        shard_id = template.to_shard_id(table_prefix, translations)
        nameserver.delete_shard(shard_id)
        # return inverse
        shard_info = template.to_shard_info(table_prefix, translations)
        TransformOperation.with(:create_shard, shard_info)
      end
    end

    class SetForwarding < ShardOp
      def expand(copy_source, involved_in_copy, batch_finish)
        # TODO: enforce that wrapper definitions match everywhere
        if !batch_finish && involved_in_copy && @wrapper_type
          copy_wrapper = ShardTemplate.new(@wrapper_type, nil, 0, '', '', [to])
          { :prepare => [SetForwarding.new(template, copy_wrapper)],
            :cleanup => [self] }
        elsif batch_finish && involved_in_copy && @wrapper_type
          copy_wrapper = ShardTemplate.new(@wrapper_type, nil, 0, '', '', [to])
          unblock_write_wrapper = ShardTemplate.new('WriteOnlyShard', nil, 0, '', '', [to])
          { :prepare => [SetForwarding.new(template, copy_wrapper)],
            :unblock_writes => [SetForwarding.new(template, unblock_write_wrapper)],
            :unblock_reads => [self] }
        else
          { :prepare => [self] }
        end
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        shard_id   = template.to_shard_id(table_prefix, translations)
        forwarding = Forwarding.new(table_id, base_id, shard_id)
        nameserver.set_forwarding(forwarding)
        # return inverse
        # NB: since forwarding changes are update-only, inverting a forwarding means
        # restoring whatever we think we're overwriting now. VERY much assumes that
        # there are no concurrent operations, which is terrifying yay!
        TransformOperation.with(:set_forwarding, forwarding)
      end
    end


    class RemoveForwarding < ShardOp
      def expand(copy_source, involved_in_copy, batch_finish)
        { (involved_in_copy ? :cleanup : :prepare) => [self] }
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        # This is a no-op in gizzard deployments, because we do not support
        # splitting/merging forwardings: thus, forwardings are always 'updated' via
        # overwriting with AddForwarding: the op exists solely for setup/teardown
        # symmetry
        # nameserver.remove_forwarding(forwarding)
        nil
      end

      def noop?
        true
      end
    end

    # a no-op that indicates a position that rollback cannot move past
    class Commit < ShardOp
      def expand(copy_source, involved_in_copy, batch_finish)
        { (involved_in_copy ? :cleanup : :prepare) => [self] }
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        # the inverse of a commit is still a commit
        TransformOperation.with(:commit, true)
      end

      def noop?
        true
      end
    end
    class CommitBegin < Commit; end
    class CommitEnd < Commit; end
  end
end
