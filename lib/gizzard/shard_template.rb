module Gizzard
  class ShardTemplate
    include Comparable

    ABSTRACT_HOST = "localhost"
    DEFAULT_WEIGHT = 1

    attr_reader :type, :weight, :source_type, :dest_type

    def initialize(type, host, weight, source_type, dest_type, children)
      @type, @host, @weight, @source_type, @dest_type, @children =
        type, host, weight, source_type || '', dest_type || '', children
    end

    def self.concrete?(type)
      !Shard::VIRTUAL_SHARD_TYPES.include? type.split('.').last
    end

    def concrete?
      self.class.concrete? type
    end

    def replicating?
      Shard::REPLICATING_SHARD_TYPES.include? type.split('.').last
    end

    def valid_copy_source?
      !Shard::INVALID_COPY_TYPES.include? type.split('.').last
    end

    def identifier
      concrete? ? "#{type}/#{host}" : type.to_s
    end

    def table_name_suffix
      Shard::SHARD_SUFFIXES[type.split('.').last]
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

    def descendants
      [self].concat children.map {|c| c.descendants }.flatten
    end

    alias flatten descendants

    def concrete_descendants
      descendants.select {|t| t.concrete? }
    end

    def copy_sources
      return [] unless self.valid_copy_source?
      self.concrete? ? [self] : children.inject([]) {|a, c| a.concat c.copy_sources }
    end

    def inspect
      to_config
    end
    alias to_s inspect

    # Concretization

    def to_shard_id(table_prefix, translations = {})
      table_prefix = [table_prefix, table_name_suffix].compact.join('_')
      shard_id     = ShardId.new(host, table_prefix)
      translations[shard_id] || shard_id
    end

    def to_shard_info(table_prefix, translations = {})
      ShardInfo.new(to_shard_id(table_prefix, translations), type, source_type, dest_type, 0)
    end

    def to_shard(table_prefix, translations = {})
      Shard.new(to_shard_info(table_prefix, translations), children.map {|c|
        c.to_shard(table_prefix, translations)
      }, weight)
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

    def shard_eql?(other)
      raise ArgumentError, "other is not a ShardTemplate" unless other.is_a? ShardTemplate

      to_a = lambda {|t| [t.host, t.type, t.source_type.to_s, t.dest_type.to_s] }
      to_a.call(self) == to_a.call(other)
    end

    def link_eql?(other)
      raise ArgumentError, "other is not a ShardTemplate" unless other.is_a? ShardTemplate

      to_a = lambda {|t| [t.host, t.type, t.source_type.to_s, t.dest_type.to_s, t.weight] }
      to_a.call(self) == to_a.call(other)
    end

    def shared_host?(other)
      raise ArgumentError, "other is not a ShardTemplate" unless other.is_a? ShardTemplate

      (self.concrete_descendants & other.concrete_descendants).length > 0
    end

    def hash
      weight.hash + host.hash + type.hash + children.hash
    end


    # Config

    def config_definition
      args = identifier.split("/")
      args << weight
      args.concat [@source_type,@dest_type] unless [@source_type, @dest_type].reject {|s| s.empty? }.empty?

      type = args.shift
      args_s = args.empty? ? "" : "(#{args.join(",")})"

      type + args_s
    end

    private :config_definition

    def to_config_struct
      if children.empty?
        config_definition
      else
        child_defs = children.map {|c| c.to_config_struct }
        { config_definition => (child_defs.length == 1 ? child_defs.first : child_defs) }
      end
    end

    def to_config
      if children.empty?
        config_definition
      else
        child_defs = children.map {|c| c.to_config }
        child_defs_s = child_defs.length == 1 ? child_defs.first : "(#{child_defs.join(", ")})"
        "#{config_definition} -> #{child_defs_s}"
      end
    end


    # Class Methods

    class << self
      def parse(string)
        definition_s, children_s = string.split(/\s*->\s*/, 2)

        children =
        if children_s.nil?
          []
        else
          list = parse_arg_list(children_s).map {|c| parse c }
          raise ArgumentError, "invalid shard config. -> given, no children found" if list.empty?
          list
        end

        template_args = parse_definition(definition_s) << children
        ShardTemplate.new(*template_args)
      end

      private

      def parse_definition(definition_s)
        type, arg_list = definition_s.split("(", 2)

        host, weight, source_type, dest_type =
          if arg_list.nil?
            nil
          else
            args = parse_arg_list("(" + arg_list)
            args.unshift nil unless concrete? type
            args
          end

        validate_host_arg(host, definition_s) if concrete? type
        validate_weight_arg(weight, definition_s)

        weight = (weight || DEFAULT_WEIGHT).to_i
        source_type ||= ""
        dest_type   ||= ""

        [type, host, weight, source_type, dest_type]
      end

      def parse_arg_list(string)
        string = string.strip
        if m = string.match(/\A\((.*)\)\Z/)
          string = m[1]
        end

        depth = 0
        results = [[]]

        string.each_char do |c|
          case c
          when ","
            if depth == 0
              results << []
              next
            end
          when "(" then depth += 1
          when ")" then depth -= 1
          end

          results.last << c
        end

        results.map {|r| r.join.strip }
      end

      def validate_weight_arg(arg, definition)
        if arg && YAML.load(arg.to_s).is_a?(String)
          raise ArgumentError, "Invalid weight #{arg} for shard in: #{definition}"
        end
      end

      def validate_host_arg(arg, definition)
        if arg.nil? || YAML.load(arg.to_s).is_a?(Numeric)
          raise ArgumentError, "Invalid host #{arg} for shard in: #{definition}"
        end
      end
    end
  end
end
