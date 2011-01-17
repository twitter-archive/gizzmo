require 'set'

module Gizzard
  class Rebalancer
    TemplateAndTree = Struct.new(:template, :forwarding, :tree)
    Bucket          = Struct.new(:template, :approx_shards, :set)

    class Bucket
      def balance; set.length - approx_shards end
      def add(e); set.add(e) end
      def delete(e); set.delete(e) end
    end

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

      @dest_templates      = dest_templates_and_weights.keys

      total_shards = @shards.length
      total_weight = dest_templates_and_weights.values.inject {|a,b| a + b }

      @result = dest_templates_and_weights.map do |template, weight|
        weight_fraction = weight / total_weight.to_f
        approx_shards   = total_shards * weight_fraction

        Bucket.new template, approx_shards, Set.new
      end
    end

    def home!
      @shards.each do |s|
        descendants = memoized_concrete_descendants(s.template)

        most_similar_templates = []
        last_cost = nil

        @dest_templates.each do |bucket|
          cost      = (memoized_concrete_descendants(bucket) - descendants).length
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
        move_shard ordered.first.template, ordered.last.set.each {|e| break e }
      end
    end

    def transformations
      return @transformations if @transformations

      home!
      rebalance!

      @transformations = {}
      @result.each do |bucket|
        bucket.set.each do |shard|
          trans = Transformation.new(shard.template, bucket.template, @copy_dest_wrapper)
          forwardings_to_trees = (@transformations[trans] ||= {})

          forwardings_to_trees.update(shard.forwarding => shard.tree)
        end
      end

      @transformations.reject! {|t, _| t.noop? }
      @transformations
    end

    def ordered_buckets
      @result.sort_by {|bucket| bucket.balance }
    end

    def memoized_concrete_descendants(t)
      @memoized_concrete_descendants ||= {}
      @memoized_concrete_descendants[t] ||= t.concrete_descendants
    end

    def bucket_disparity
      ordered = ordered_buckets
      ordered.last.balance - ordered.first.balance
    end

    def move_shard(template, shard)
      @result.each do |bucket|
        if bucket.template == template
          bucket.add shard
        else
          bucket.delete shard
        end
      end
    end
  end
end
