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
        descendants           = s.template.concrete_descendants
        most_similar_template = @buckets.
          map        {|b| [(b.concrete_descendants - descendants).length, b] }.
          inject({}) {|h, (cost, b)| h.update(cost => [b]) {|k,a,b| a.concat b } }.
          to_a.
          sort_by    {|a| a.first }.
          first.
          last.
          choice

        move_shard most_similar_template, s
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
