module Gizzard
  class ShardTemplate
    include Comparable

    ABSTRACT_HOST = "localhost"
    DEFAULT_WEIGHT = 1

    VIRTUAL_SHARD_TYPES = [
      "FailingOverShard",
      "ReplicatingShard",
      "ReadOnlyShard",
      "WriteOnlyShard",
      "BlockedShard",
    ]

    REPLICATING_SHARD_TYPES = ["ReplicatingShard", "FailingOverShard"]

    INVALID_COPY_TYPES = ["ReadOnlyShard", "WriteOnlyShard", "BlockedShard"]

    SHARD_SUFFIXES = {
      "FailingOverShard" => 'replicating',
      "ReplicatingShard" => 'replicating',
      "ReadOnlyShard" => 'read_only',
      "WriteOnlyShard" => 'write_only',
      "BlockedShard" => 'blocked'
    }

    attr_reader :type, :weight, :source_type, :dest_type

    def initialize(type, host, weight, source_type, dest_type, children)
      @type, @host, @weight, @source_type, @dest_type, @children =
        type, host, weight, source_type || '', dest_type || '', children
    end

    def self.concrete?(type)
      !VIRTUAL_SHARD_TYPES.include? type.split('.').last
    end

    def concrete?
      self.class.concrete? type
    end

    def replicating?
      REPLICATING_SHARD_TYPES.include? type
    end

    def identifier
      concrete? ? "#{type}:#{host}" : type.to_s
    end

    def host
      if concrete?
        @host
      elsif children.length == 1
        children.first.host
      else
        ABSTRACT_HOST
      end
    end

    def children
      @children.sort { |a, b| b <=> a }
    end

    def inspect
      weight_inspect = weight.nil? ? "" : " #{weight}"
      child_inspect = children.empty? ? "" : " #{children.inspect}"
      "(#{identifier}#{weight_inspect}#{child_inspect})"
    end


    # Similarity/Equality

    def <=>(other)
      raise ArgumentError, "other is not a ShardTemplate" unless other.is_a? ShardTemplate

      to_a = lambda {|t| [t.host, t.type, t.source_type.to_s, t.dest_type.to_s, t.weight] }

      if (cmp = to_a.call(self) <=> to_a.call(other)) == 0
        children <=> other.children
      else
        cmp
      end
    end

    def eql?(other)
      return false unless other.is_a? ShardTemplate
      (self <=> other) == 0
    end

    def hash
      weight.hash + host.hash + type.hash + children.hash
    end


    # Config

    def to_config
      definition = [identifier, weight].compact.join(":")

      if children.empty?
        definition
      else
        child_defs = children.map {|c| c.to_config }
        child_defs = child_defs.first if child_defs.length == 1
        { definition => child_defs }
      end
    end


    # Class Methods

    class << self
      def from_config(config, conf_tree)
        shard, child_configs = parse_link_struct(conf_tree)
        type, host, weight   = parse_shard_definition(shard)
        children             = Array(child_configs).map { |child| from_config(config, child) }

        new(type, host, weight, config.source_type, config.destination_type, children)
      end

      private

      def parse_link_struct(obj)
        if obj.is_a? String
          [obj, nil]
        elsif obj.is_a? Hash and obj.length == 1
          [obj.keys.first, obj.values.first]
        else
          raise ArgumentError, "invalid shard tree: #{obj.inspect}"
        end
      end

      def parse_shard_definition(definition)
        type, arg1, arg2 = definition.split(":")

        host, weight =
          unless concrete? type
            if arg2 or YAML.load(arg1.to_s).is_a? String
              raise ArgumentError, "cannot specify a host for #{type} shard in: #{definition.inspect}"
            end
            [nil, (arg1 || DEFAULT_WEIGHT).to_i]
          else
            if arg1.nil? or YAML.load(arg1.to_s).is_a? Numeric
              raise ArgumentError, "must specify a host for #{type} shard in: #{definition.inspect}"
            end
            [arg1, (arg2 || DEFAULT_WEIGHT).to_i]
          end

        [type, host, weight]
      end
    end
  end
end
