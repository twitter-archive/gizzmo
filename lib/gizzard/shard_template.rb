require 'yaml'

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

    def shard_tag
      Shard::SHARD_TAGS[type.split('.').last]
    end

    def host
      if concrete?
        @host
      elsif replicating?
        ABSTRACT_HOST
      else
        children.first.host
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

      if ((cmp = self.host <=> other.host) != 0); return cmp end
      if ((cmp = self.type <=> other.type) != 0); return cmp end
      if ((cmp = self.source_type.to_s <=> other.source_type.to_s) != 0); return cmp end
      if ((cmp = self.dest_type.to_s <=> other.dest_type.to_s) != 0); return cmp end
      if ((cmp = self.weight <=> other.weight) != 0); return cmp end
      return self.children <=> other.children
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

      self.concrete_descendants.each do |s|
        other.concrete_descendants.each do |o|
          return true if s.shard_eql? o
        end
      end

      false
    end

    def contains_shard_type?(other)
      descendants.map {|d| d.type }.include? other
    end

    def hash
      return @hash if @hash
      @hash = weight.hash + host.hash + type.hash + children.hash
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
      if ShardTemplate.options[:simple]
        to_simple_config
      else
        to_complex_config
      end
    end
    
    def to_complex_config
      if children.empty?
        config_definition
      else
        child_defs = children.map {|c| c.to_complex_config }
        child_defs_s = child_defs.length == 1 ? child_defs.first : "(#{child_defs.join(", ")})"
        "#{config_definition} -> #{child_defs_s}"
      end
    end

    def to_simple_config(tag="")
      if children.empty? && concrete?
        tag += "+#{shard_tag}" if shard_tag
        "#{host}#{tag if !tag.empty?}"
      elsif !children.empty?
        if replicating?
          children.map {|c| c.to_simple_config }.join(", ")
        else
          children.map {|c| c.to_simple_config("#{tag}#{'+' + shard_tag.to_s if shard_tag}")}
        end
      else
        ""
      end
    end

    # Class Methods

    class << self

      def configure(options)
        @@options ||= {}
        @@options.merge!(options)
      end

      def options
        @@options ||= {}
        @@options[:replicating] ||= "ReplicatingShard"
        #@@options[:source_type] ||= "BIGINT UNSIGNED"
        #@@options[:dest_type] ||= "BIGINT UNSIGNED"
        @@options
      end

      def parse(string)
        if options[:simple]
          parse_simple(string)
        else
          parse_complex(string)
        end
      end

      private

      def parse_simple(definition_s)
        shards = definition_s.split(",")
        templates = []
        shards.each do |s|
          s.strip!
          host, *tags = s.split("+")
          templates << build_nested_template(host, tags)
        end
        ShardTemplate.new(options[:replicating], nil, 1, "", "", templates)
      end

      def build_nested_template(host, tags)
        if tags.empty?
          return ShardTemplate.new(options[:concrete], host, 1, options[:source_type], options[:dest_type], [])
        end

        tag = tags.shift
        type = Shard::SHARD_TAGS.invert[tag]

        if type == "BlackHoleShard" # end at blackhole shards immediately since they're concrete
          return ShardTemplate.new(type, ABSTRACT_HOST, 1, "", "", [])
        end

        if type
          return ShardTemplate.new(type, nil, 1, "", "", [build_nested_template(host, tags)])
        end

        []
      end


      def parse_complex(string)
        definition_s, children_s = string.split(/\s*->\s*/, 2)

        children =
        if children_s.nil?
          []
        else
          list = parse_arg_list(children_s).map {|c| parse_complex c }
          raise ArgumentError, "invalid shard config. -> given, no children found" if list.empty?
          list
        end

        template_args = parse_complex_definition(definition_s) << children
        ShardTemplate.new(*template_args)
      end

      def parse_complex_definition(definition_s)
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
