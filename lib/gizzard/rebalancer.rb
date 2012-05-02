require 'set'

module Gizzard
  class Rebalancer
    TemplateAndTree = Struct.new(:template, :forwarding, :tree)
    Bucket          = Struct.new(:template, :approx_shards, :set)

    class Bucket
      def balance; set.length - approx_shards end
      def add!(es); set.merge(es) end
      def merge!(es); set.merge(es) end
      # removes n 'random' elements without set lookups
      def take!(n)
        removed = []
        set.reject! do |e|
          if (n = n-1) >= 0
            # remove first n elements
            removed << e
            true
          else
            # preserve remainder
            return removed
          end
        end
        raise "Not enough elements!"
      end
    end

    # steps for rebalancing.
    #
    # 1. get a list of forwarding/template associations
    # 2. get a list of destination templates and weights
    # 3. order shards by weight. (ascending or descending)
    # 4. put shards in destinations based on reducing number of copies required.
    # 5.

    def initialize(forwardings_to_trees, dest_templates_and_weights, wrapper, batch_finish)
      @copy_dest_wrapper = wrapper
      @batch_finish = batch_finish
      @shards = forwardings_to_trees.map do |forwarding, tree|
        TemplateAndTree.new(tree.template, forwarding, tree)
      end.flatten

      @dest_templates      = dest_templates_and_weights.keys

      total_shards = @shards.length
      total_weight = dest_templates_and_weights.values.inject {|a,b| a + b }

      @result = dest_templates_and_weights.map do |template, weight|
        weight_fraction = weight / total_weight.to_f
        approx_shards   = (total_shards * weight_fraction).round

        Bucket.new template, approx_shards, Set.new
      end
    end

    def home!
      # list of [template, shards] in descending length of shards
      templates_to_shards =
        @shards.group_by do |shard|
          shard.template
        end.sort_by {|(_,ss)| ss.length * -1 }

      templates_to_shards.each do |(template, shards)|
        descendants = memoized_concrete_descendants(template)

        most_similar_buckets = []
        last_cost = nil

        @result.each do |bucket|
          cost      = (memoized_concrete_descendants(bucket.template) - descendants).length
          last_cost = cost if last_cost.nil?

          if cost == last_cost
            most_similar_buckets << bucket
          elsif cost < last_cost
            last_cost = cost
            most_similar_buckets = [bucket]
          end
        end

        dest_bucket = most_similar_buckets.min_by {|b| b.balance }

        dest_bucket.merge! shards
      end
    end

    def rebalance!
      while (to_move = shards_to_move(min_and_max = min_and_max_buckets)) >= 1
        dest_bucket = min_and_max.first
        src_bucket = min_and_max.last
        move_shards! to_move.floor, src_bucket, dest_bucket
      end
    end

    def transformations
      return @transformations if @transformations

      home!
      rebalance!

      @transformations = {}
      @result.each do |bucket|
        bucket.set.each do |shard|
          trans = Transformation.new(shard.template, bucket.template, @copy_dest_wrapper, false, @batch_finish)
          forwardings_to_trees = (@transformations[trans] ||= {})

          forwardings_to_trees.update(shard.forwarding => shard.tree)
        end
      end

      @transformations.reject! {|t, _| t.noop? }
      @transformations
    end

    # a tuple of the minimum and maximum buckets (which might be the same bucket)
    def min_and_max_buckets
      @result.minmax_by {|bucket| bucket.balance }
    end

    def memoized_concrete_descendants(t)
      @memoized_concrete_descendants ||= {}
      @memoized_concrete_descendants[t] ||= t.concrete_descendants
    end

    def shards_to_move(min_and_max)
      min_balance = min_and_max.first.balance
      max_balance = min_and_max.last.balance
      [
        -min_balance, # number of shards the min bucket needs
        max_balance   # number of shards the max bucket can spare
      ].min
    end

    def move_shards!(shards_to_move, src_bucket, dest_bucket)
      shards = src_bucket.take!(shards_to_move)
      dest_bucket.add! shards
    end
  end
end
