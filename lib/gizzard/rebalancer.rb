require 'set'

module Gizzard
  class Rebalancer
    TemplateAndTree = Struct.new(:template, :forwarding, :tree)

    # steps for rebalancing.
    #
    # 1. get a list of forwarding/template associations
    # 2. get a list of destination templates and weights
    # 3. order shards by weight. (ascending or descending)
    # 4. put shards in destinations based on reducing number of copies required.
    # 5.

    def initialize(forwardings_to_trees, dest_templates_and_weights, wrapper)
      @copy_dest_wrapper = wrapper
      @shards = forwardings_to_trees.map do |forwarding, tree|
        TemplateAndTree.new(tree.template, forwarding, tree)
      end.flatten

      @buckets = dest_templates_and_weights.keys
      @result  = @buckets.inject({}) {|h,b| h.update b => Set.new }
    end

    def home!
      @shards.each do |s|
        descendants = memoized_concrete_descendants(s.template)

        most_similar_templates = []
        last_cost = nil

        @buckets.each do |bucket|
          cost      = (bucket_concrete_descendants(bucket) - descendants).length
          last_cost = cost if last_cost.nil?

          if cost == last_cost
            most_similar_templates << bucket
          elsif cost < last_cost
            last_cost = cost
            most_similar_templates = [bucket]
          end
        end

        move_shard most_similar_templates.choice, s
      end
    end

    def rebalance!
      while bucket_disparity > 1
        ordered = ordered_buckets
        move_shard ordered.first.first, ordered.last.last.each {|e| break e }
      end
    end

    def transformations
      return @transformations if @transformations

      home!
      rebalance!

      @transformations = {}
      @result.each do |template, shards|
        shards.each do |shard|
          trans = Transformation.new(shard.template, template, @copy_dest_wrapper)
          forwardings_to_trees = (@transformations[trans] ||= {})

          forwardings_to_trees.update(shard.forwarding => shard.tree)
        end
      end

      @transformations.reject! {|t, _| t.noop? }
      @transformations
    end

    def ordered_buckets
      @result.sort_by {|bucket, shards| shards.length }
    end

    def memoized_concrete_descendants(t)
      @memoized_concrete_descendants ||= {}
      @memoized_concrete_descendants[t] ||= t.concrete_descendants
    end

    def bucket_disparity
      ordered = ordered_buckets
      ordered.last.last.length - ordered.first.last.length
    end

    def move_shard(bucket, shard)
      @result.each {|_, ss| ss.delete shard }
      @result[bucket].add shard
    end
  end
end
