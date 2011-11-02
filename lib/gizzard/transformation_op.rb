module Gizzard
  module Transformation::Op
    class BaseOp

      def initialize(logger=nil)
        @logger = logger
      end

      def inverse?(other)
        Transformation::OP_INVERSES[self.class] == other.class
      end

      def eql?(other)
        self.class == other.class
      end

      alias == eql?

      def inspect
        templates = (is_a?(LinkOp) ? [from, to] : [template]).map {|t| t.identifier }.join(" -> ")
        name      = Transformation::OP_NAMES[self.class]
        "#{name}(#{templates})"
      end

      def <=>(other)
        Transformation::OP_PRIORITIES[self.class] <=> Transformation::OP_PRIORITIES[other.class]
      end

      def involved_shards(table_prefix, translations)
        []
      end

      def log_command table_id, base_id, table_prefix, translations
        @logger.write(self, [table_id, base_id, table_prefix, translations]) if @logger
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

      def involved_shards(table_prefix, translations)
        [to.to_shard_id(table_prefix, translations)]
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        from_shard_id = from.to_shard_id(table_prefix, translations)
        to_shard_id   = to.to_shard_id(table_prefix, translations)

        nameserver.mark_shard_busy(to_shard_id, BUSY)
        nameserver.copy_shard(from_shard_id, to_shard_id)
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

      def initialize(from, to, logger=nil)
        @from = from
        @to   = to
        super logger
      end

      def inverse?(other)
        super && self.from.link_eql?(other.from) && self.to.link_eql?(other.to)
      end

      def inverse
        inv_class = Transformation::OP_INVERSES[self.class]
        return nil if inv_class.nil?
        inv_class.new(from, to)
      end

      def eql?(other)
        super && self.from.link_eql?(other.from) && self.to.link_eql?(other.to)
      end

      def serialize
        [from,to]
      end

      def self.deserialize from, to
        self.new from, to
      end
    end

    class AddLink < LinkOp
      def expand(copy_source, involved_in_copy, wrapper_type)
        if involved_in_copy
          wrapper = ShardTemplate.new(wrapper_type, to.host, to.weight, '', '', [to])
          { :prepare => [AddLink.new(from, wrapper, @logger)],
            :cleanup => [self, RemoveLink.new(from, wrapper, @logger)] }
        else
          { :prepare => [self] }
        end
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        log_command table_id, base_id, table_prefix, translations
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
        log_command table_id, base_id, table_prefix, translations
        from_shard_id = from.to_shard_id(table_prefix, translations)
        to_shard_id   = to.to_shard_id(table_prefix, translations)

        nameserver.remove_link(from_shard_id, to_shard_id)
      end
    end

    class ShardOp < BaseOp
      attr_reader :template

      def initialize(template, logger=nil)
        @template = template
        super logger
      end

      def inverse?(other)
        super && self.template.shard_eql?(other.template)
      end

      def inverse
        inv_class = Transformation::OP_INVERSES[self.class]
        return nil if inv_class.nil?
        inv_class.new(template)
      end

      def eql?(other)
        super && self.template.shard_eql?(other.template)
      end

      def serialize
        [template]
      end

      def self.deserialize template
        self.new template
      end
    end

    class CreateShard < ShardOp
      def expand(copy_source, involved_in_copy, wrapper_type)
        if involved_in_copy
          wrapper = ShardTemplate.new(wrapper_type, template.host, template.weight, '', '', [template])
          { :prepare => [self, CreateShard.new(wrapper, @logger), AddLink.new(wrapper, template, @logger)],
            :cleanup => [RemoveLink.new(wrapper, template, @logger), DeleteShard.new(wrapper, @logger)],
            :copy => [CopyShard.new(copy_source, template)] }
        else
          { :prepare => [self] }
        end
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        log_command table_id, base_id, table_prefix, translations
        nameserver.create_shard(template.to_shard_info(table_prefix, translations))
      end
    end

    class DeleteShard < ShardOp
      def expand(copy_source, involved_in_copy, wrapper_type)
        { (involved_in_copy ? :cleanup : :prepare) => [self] }
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        log_command table_id, base_id, table_prefix, translations
        nameserver.delete_shard(template.to_shard_id(table_prefix, translations))
      end
    end

    class SetForwarding < ShardOp
      def expand(copy_source, involved_in_copy, wrapper_type)
        if involved_in_copy
          wrapper = ShardTemplate.new(wrapper_type, nil, 0, '', '', [to])
          { :prepare => [SetForwarding.new(template, wrapper, @logger)],
            :cleanup => [self] }
        else
          { :prepare => [self] }
        end
      end

      def apply(nameserver, table_id, base_id, table_prefix, translations)
        log_command table_id, base_id, table_prefix, translations
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
        log_command table_id, base_id, table_prefix, translations
        # shard_id   = template.to_shard_id(table_prefix, translations)
        # forwarding = Forwarding.new(table_id, base_id, shard_id)
        # nameserver.remove_forwarding(forwarding)
      end
    end
  end
end
