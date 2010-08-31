module Gizzard
  class ShardTemplate
    include Comparable

    ABSTRACT_HOST = "localhost"
    DEFAULT_WEIGHT = 1

    GIZZARD_SHARD_TYPES = [
      "com.twitter.gizzard.shards.ReplicatingShard",
      "com.twitter.gizzard.shards.ReadOnlyShard",
      "com.twitter.gizzard.shards.WriteOnlyShard",
      "com.twitter.gizzard.shards.BlockedShard",
      "ReplicatingShard",
      "ReadOnlyShard",
      "WriteOnlyShard",
      "BlockedShard",
    ]

    INVALID_COPY_TYPES = ["ReadOnlyShard", "WriteOnlyShard", "BlockedShard"]

    attr_reader :type, :weight

    def initialize(type, host, weight, children)
      @type, @host, @weight, @children = type, host, weight, children
    end

    def concrete?
      !GIZZARD_SHARD_TYPES.include? type
    end

    def valid?
      return false if concrete? and !children.empty?
      return false if !replicating? && children.length > 1
      return false if replicating? && children.any? { |child| child.replicating? }

      children.each { |child| return false unless child.valid? }

      true
    end

    def replicating?
      type =~ /ReplicatingShard/
    end

    def short_type
      type.split(".").last
    end

    def identifier
      replicating? ? type.to_s : "#{type}:#{host}"
    end

    def host
      if concrete?
        @host
      elsif !replicating?
        children.first.host
      else
        ABSTRACT_HOST
      end
    end

    def children
      @children.sort { |a, b| b <=> a }
    end

    def descendant_identifiers
      ids = children.map { |c| c.descendant_identifiers }.flatten
      ids << identifier if concrete?
      ids.uniq.sort
    end

    def copy_sources(multiplier = 1.0)
      return {} if INVALID_COPY_TYPES.include? type

      if concrete?
        { self => multiplier }
      else
        total_weight = children.map {|c| c.weight }.inject {|a,b| a+b }.to_f
        children.inject({}) do |sources, child|
          share = total_weight.zero? ? 0 : (child.weight / total_weight * multiplier)
          sources.merge child.copy_sources(share)
        end
      end
    end

    def copy_source
      copy_sources.to_a.sort {|a,b| a.last <=> b.last }.first.first
    end

    def inspect
      weight_inspect = weight.nil? ? "" : " #{weight}"
      child_inspect = children.empty? ? "" : " #{children.inspect}"
      "(#{identifier}#{weight_inspect}#{child_inspect})"
    end


    # Materialization

    def to_shard_id(table_name)
      Thrift::ShardId.new(host, concrete? ? table_name : table_name + "_" + short_type)
    end

    def to_shard_info(config, table_name)
      Thrift::ShardInfo.new(to_shard_id(table_name), type, config.source_type, config.destination_type, 0)
    end


    # Similarity/Equality

    include Comparable

    def similar?(other)
      return false unless other.is_a? ShardTemplate
      (self.descendant_identifiers & other.descendant_identifiers).length > 0
    end

    def <=>(other, deep = true)
      raise ArgumentError, "other is not a ShardTemplate" unless other.is_a? ShardTemplate

      if (cmp = [weight, host, type.to_s] <=> [other.weight, other.host, other.type.to_s]) == 0
        # only sort children if necessary...
        deep ? children <=> other.children : 0
      else
        cmp
      end
    end

    def eql?(other, deep = true)
      return false unless other.is_a? ShardTemplate
      (self.<=>(other,deep)).zero?
    end

    def hash
      weight.hash + host.hash + type.hash + children.hash
    end


    # Config

    def to_config
      weight_def = (weight  == DEFAULT_WEIGHT) ? nil : weight
      definition = [identifier, weight_def].compact.join(":")

      if children.empty?
        definition
      else
        child_defs = children.map {|c| c.to_config }
        child_defs = child_defs.first if child_defs.length == 1
        { definition => child_defs }
      end
    end


    # Class Methods

    module Introspection
      def existing_template_map(nameserver)
        forwardings = nameserver.get_forwardings
        roots = forwardings.map { |f| f.shard_id }
        links = collect_links(nameserver, roots)
        shard_map = collect_shards(nameserver, links)

        trees = Hash.new { |h,k| h[k] = [] }

        roots.each do |root|
          tree = build_tree(root, DEFAULT_WEIGHT, shard_map, links)
          trees[tree] << root.table_prefix
        end
        trees
      end

      private

      def build_tree(root_id, link_weight, shard_repo, link_repo)
        host = root_id.hostname

        children = link_repo[root_id].map do |child_id, child_weight|
          build_tree(child_id, child_weight, shard_repo, link_repo)
        end

        p shard_repo
        new(shard_repo[root_id].class_name, host, link_weight, children)
      end

      def collect_links(nameserver, roots)
        links = Hash.new { |h, k| h[k] = [] }

        collector = lambda do |parent|
          children = nameserver.list_downward_links(parent).map do |link|
            links[link.up_id] << [link.down_id, link.weight]
            link.down_id
          end

          children.each { |child| collector.call(child) }
        end

        roots.each {|root| collector.call(root) }
        links
      end

      def collect_shards(nameserver, links)
        shard_ids = links.keys + links.values.inject([]) do |ids, nodes|
          nodes.each { |id, weight| ids << id }
          ids
        end

        shard_ids.inject({}) { |h, id| h.update id => nameserver.get_shard(id) }
      end
    end

    extend Introspection


    module Configuration
      def from_config(obj)
        shard, children = parse_link_struct(obj)
        type, host, weight = parse_shard_definition(shard)
        new(type, host, weight, Array(children).map { |child| from_config(child) })
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
          if GIZZARD_SHARD_TYPES.include? type
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

    extend Configuration
  end
end
