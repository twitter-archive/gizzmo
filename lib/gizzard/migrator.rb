module Gizzard
  class MigratorConfig
    attr_accessor :prefix, :table_id, :source_type, :destination_type, :forwarding_space, :forwarding_space_min

    def initialize(opts = {})
      opts.each {|(k,v)| send("#{k}=", v) if respond_to? "{k}=" }
    end
  end

  class Migrator
    BALANCE_TOLERANCE = 1

    attr_reader :configured_templates, :existing_map, :existing_templates, :total_shards

    # populated via derive_changes
    attr_reader :new_templates, :unrecognized_templates, :similar_templates, :unchanged_templates

    def initialize(existing_map, config_templates, default_total_shards, config)
      @configured_templates = config_templates
      @existing_map = existing_map
      @existing_templates = existing_map.keys
      @total_shards = @existing_map.values.map { |a| a.length }.inject { |a, b| a + b } || default_total_shards
      @config = config
      derive_changes
    end

    def transformations
      return @transformations if @transformations

      # no changes
      return @transformations = [] if similar_templates.empty? and unrecognized_templates.empty? and new_templates.empty?

      configured_map = configured_templates.inject({}) {|h, t| h.update t => [] }

      @transformations = []

      if existing_templates.empty?
        # no forwardings exist, we must populate the forwarding index.
        forwardings = generate_new_forwardings(total_shards)

        # add the new table ids to a member of the configured map. will
        # be rebalanced later.
        configured_map.values.first.concat forwardings.values

        @transformations << ForwardingTransformation.new(@config.table_id, forwardings)
      end

      # map the unchanged templates straight over
      move_unchanged(existing_map, configured_map)

      # map similar templates over to their new versions
      move_similar(existing_map, configured_map)

      # move shards from unrecognized templates to new templates (or
      # existing ones)
      move_unrecognized_to_new(existing_map, configured_map)

      # rebalance
      rebalance_shards(configured_map)

      # transformation generation
      @transformations = generate_transformations(existing_map, configured_map) + @transformations
    end

    def generate_new_forwardings(shard_count)
      forwardings = {}
      step_size = @config.forwarding_space / shard_count
      bases = (0...shard_count).map { |i| @config.forwarding_space_min + (i * step_size) }

      bases.each_with_index do |base_id, i|
        table_name = [ @config.prefix, @config.table_id, "%04d" % i ].compact.join("_")
        forwardings[base_id] = table_name
      end

      forwardings
    end

    def prepare!(nameserver)
      transformations.each {|t| t.prepare! nameserver }
    end

    def copy!(nameserver)
      transformations.each {|t| t.copy! nameserver }
    end

    def wait_for_copies(nameserver)
      transformations.each {|t| t.wait_for_copies nameserver }
    end

    def cleanup!(nameserver)
      transformations.each {|t| t.cleanup! nameserver }
    end

    private

    def move_unchanged(existing, configured)
      unchanged_templates.each {|u| configured[u] = existing[u].dup }
    end

    def move_similar(existing, configured)
      similar_templates.each {|from, to| configured[to] = existing[from].dup }
    end

    def move_unrecognized_to_new(existing, configured)
      # duplicate so we can mutate our copy
      unrecognized = unrecognized_templates.dup

      # for each new template, grab an unrecognized one's shards
      # and pop it off
      new_templates.each do |n|
        if u = unrecognized.pop
          configured[n] = existing[u].dup
        end
      end

      # if there are any unrecognized templates for which we haven't
      # moved shards over, add their shards to the first template. they will get rebalanced later
      leftover_shards = unrecognized.inject([]) {|a, u| a.concat existing[u] }

      configured.last.concat leftover_shards unless leftover_shards.empty?
    end

    def rebalance_shards(configured)
      until shards_balanced? configured
        smallest(configured) << largest(configured).pop
      end
    end

    def generate_transformations(existing, configured)
      existing_shards = shards_to_templates(existing)
      configured_shards = shards_to_templates(configured)

      # find the list of shards which have moved, and emit a
      # transformation for each one.
      (configured_shards.to_a - existing_shards.to_a).inject({}) do |transformations, (shard, to)|
        from = existing_shards[shard]
        (transformations[[from, to]] ||= Transformation.new(from, to, [], @config)).shard_ids << shard
        transformations
      end.values
    end

    def shards_balanced?(template_map)
      sorted_sizes = template_map.values.map {|s| s.length }.uniq.sort.reverse
      sorted_sizes.first - sorted_sizes.last <= BALANCE_TOLERANCE
    end

    def smallest(template_map)
      template_map.values.sort {|a,b| a.length <=> b.length }.first
    end

    def largest(template_map)
      template_map.values.sort {|a,b| b.length <=> a.length }.first
    end

    def shards_to_templates(templates_to_shards)
      templates_to_shards.inject({}) do |h, (template, shards)|
        shards.each {|shard| h[shard] = template }; h
      end
    end

    def derive_changes
      @unrecognized_templates, @new_templates, related_templates =
        split_set(existing_templates, configured_templates) {|a, b| a.similar? b }

      @similar_templates = related_templates.reject {|(a,b)| a == b }
      @unchanged_templates = related_templates.keys - @similar_templates.keys
    end

    def split_set(a, b, &predicate)
      in_a = a.dup
      in_b = b.dup
      overlap = {}

      in_a.each_with_index do |a, a_i|
        in_b.each_with_index do |b, b_i|
          if predicate.call(a, b)
            overlap[a] = b
            in_a[a_i] = in_b[b_i] = nil
          end
        end
      end

      [in_a.compact, in_b.compact, overlap]
    end
  end
end
