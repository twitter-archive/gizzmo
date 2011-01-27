require 'set'

module Gizzard
  class Rebalancer
    TemplateAndTree = Struct.new(:template, :forwarding, :tree)
    Bucket          = Struct.new(:template, :approx_shards, :set)

    class Bucket
      def balance; set.length - approx_shards end
      def add(e); set.add(e) end
      def merge(es); set.merge(es) end
      def delete(e); set.delete(e) end
    end

    # steps for rebalancing.
    #
    # 1. get a list of forwarding/template associations
    # 2. get a list of destination templates and weights
    # 3. order shards by weight. (ascending or descending)
    # 4. put shards in destinations based on reducing number of copies required.
    # 5.

    def initialize(forwardings_to_trees, dest_templates_and_weights, shard_weight_filename, strategy, tolerance, wrapper)
      @strategy = strategy
      @tolerance = tolerance
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

      load_shard_weights(shard_weight_filename)
    end

    def load_shard_weights(filename)
      @shard_weights = if filename.nil?
          {}
        else
          Hash[*File.read(filename).split("\n").map { |line| line.split(/\s*:\s*/, 2) }.flatten]
        end
    end

    def get_shard_weight(shard)
      @shard_weights[shard.forwarding.shard_id.to_unix].to_i || 1
    end

    def home!
      # list of [template, shards] in descending length of shards
      templates_to_shards =
        @shards.inject({}) do |h, shard|
          (h[shard.template] ||= []) << shard; h
        end.sort_by {|(_,ss)| ss.length * -1 }

      @template_to_bucket = Hash[*@result.map {|b| [b.template, b]}.flatten]
      templates_to_shards.each do |(template, shards)|
        @template_to_bucket[template] ||= Bucket.new template, 0.0, Set.new
        @template_to_bucket[template].merge(shards)
      end

#      templates_to_shards.each do |(template, shards)|
#        descendants = memoized_concrete_descendants(template)
#
#        most_similar_buckets = []
#        last_cost = nil
#
#        @result.each do |bucket|
#          cost      = (memoized_concrete_descendants(bucket.template) - descendants).length
#          last_cost = cost if last_cost.nil?
#
#          if cost == last_cost
#            most_similar_buckets << bucket
#          elsif cost < last_cost
#            last_cost = cost
#            most_similar_buckets = [bucket]
#          end
#        end
#
#        dest_bucket = most_similar_buckets.sort_by {|b| b.balance }.first
#
#        dest_bucket.merge shards
#      end
    end

    def rebalance_sticky_greedy!
      template_weights = Hash[*@result.map {|b| [b.template, 0]}.flatten]

      threshold = @shards.inject(0) {|h, s| h + get_shard_weight(s)} / @result.length * @tolerance
      puts threshold

      total = 0.0
      ordered_shards(@shards).each do |shard|
        average = total/@result.length
        current = shard.template
        template_weight = template_weights[current] || 9999999999
        shard_weight = get_shard_weight(shard)
        puts "#{template_weight + shard_weight} > #{average + threshold}"
        if template_weight + shard_weight > average + threshold
          smallest = ordered_buckets(template_weights).first
          if template_weights[current] != template_weights[smallest]
            puts "move"
            @template_to_bucket[current].delete(shard)
            @template_to_bucket[smallest].add(shard)
            template_weights[smallest] += shard_weight
          else
            puts "stay"
            template_weights[current] += shard_weight 
          end
        else
          puts "stay"
          template_weights[current] += shard_weight
        end
        total += shard_weight
      end

      average = total/@result.length
      upper = average + threshold
      lower = average - threshold
      puts "#{lower} < #{upper}"
      dead_templates = {}
      template_weights.each do |k, v|
        if v < lower or v > upper
          dead_templates[k] = template_weights.delete(k)
        end
      end
      print_template_weights(template_weights)
      puts "dead templates:"
      print_template_weights(dead_templates)
      puts "end dead templates"
    end

    def rebalance_greedy!
      template_weights = Hash[*@result.map {|b| [b.template, 0]}.flatten]

      ordered_shards(@shards).each do |shard|
        shard_weight = get_shard_weight(shard)
        current = shard.template
        smallest = ordered_buckets(template_weights).first
        if template_weights[current] != template_weights[smallest]
          puts "move"
          @template_to_bucket[current].delete(shard)
          @template_to_bucket[smallest].add(shard)
          template_weights[smallest] += shard_weight
        else
          puts "stay"
          template_weights[current] += shard_weight
        end
      end
      print_template_weights(template_weights)
    end

    def rebalance_minimal!
      templates = @result.map {|b| b.template}
      dead_templates = {}
      template_weights = templates.inject({}) {|h, t| h[t] = 0; h}
      template_weights = @shards.inject({}) do |h, s|
        h[s.template] ||= 0
        h[s.template] += get_shard_weight(s)
        h
      end
      print_template_weights(template_weights)

      average = template_weights.keys.inject(0) {|s, t| s + template_weights[t]} / templates.length
      upper = average * (1.0 + @tolerance)
      lower = average * (1.0 - @tolerance)

      moved_shards = Set.new

      sorted_templates = ordered_templates(template_weights)
      while template_weights.length > 1 and (template_weights[sorted_templates[0]] < lower or template_weights[sorted_templates[-1]] > upper) and moved_shards.length < @shards.length
        weight, shard_to_move = nil, nil
        big_shards = ordered_shards(@template_to_bucket[sorted_templates[-1]].set).reverse
        big_shards.each do |shard|
          puts 'a'
          if moved_shards.include?(shard)
            next
          end
          weight = get_shard_weight(shard)
          if template_weights[sorted_templates[-1]] - weight >= lower
            shard_to_move = shard
            puts 'b'
            break
          end
        end
        puts 'c'

        if shard_to_move.nil?
          weight = template_weights.delete(sorted_templates[-1])
          dead_templates[sorted_templates[-1]] = weight
        else
          @template_to_bucket[sorted_templates[-1]].delete(shard_to_move)
          @template_to_bucket[sorted_templates[0]].add(shard_to_move)
          template_weights[sorted_templates[-1]] -= weight
          template_weights[sorted_templates[0]] += weight
          moved_shards.add(shard_to_move)
          puts "move"
        end
        sorted_templates = ordered_templates(template_weights)
        puts "(#{template_weights[sorted_templates[0]]} < #{lower} or #{template_weights[sorted_templates[-1]]} > #{upper}) and #{moved_shards.length} < #{@shards.length}"
      end
      print_template_weights(template_weights)
      puts "dead templates:"
      print_template_weights(dead_templates)
      puts "end dead templates"
    end

   def rebalance_minimal_aggressive!
      templates = @result.map {|b| b.template}
      dead_templates = {}
      template_weights = templates.inject({}) {|h, t| h[t] = 0; h}
      template_weights = @shards.inject({}) do |h, s|
        h[s.template] ||= 0
        h[s.template] += get_shard_weight(s)
        h
      end
      print_template_weights(template_weights)

      average = template_weights.keys.inject(0) {|s, t| s + template_weights[t]} / templates.length
      upper = average * (1.0 + @tolerance)
      lower = average * (1.0 - @tolerance)

      moved_shards = Set.new

      sorted_templates = ordered_templates(template_weights)
      while template_weights.length > 1 and (template_weights[sorted_templates[0]] < lower or template_weights[sorted_templates[-1]] > upper) and moved_shards.length < @shards.length
        weight, shard_to_move, movable_shard = nil, nil, nil
        big_shards = ordered_shards(@template_to_bucket[sorted_templates[-1]].set).reverse
        big_shards.each do |shard|
          puts 'a'
          if moved_shards.include?(shard)
            next
          end
          movable_shard = shard
          weight = get_shard_weight(shard)
          if template_weights[sorted_templates[-1]] - weight >= lower
            shard_to_move = shard
            puts 'b'
            break
          end
        end
        puts 'c'

        if movable_shard.nil?
          weight = template_weights.delete(sorted_templates[-1])
          dead_templates[sorted_templates[-1]] = weight
          puts "stay"
        else
          to_template = sorted_templates[0]
          if shard_to_move.nil?
            shard_to_move = movable_shard
            average_sort_templates = template_weights.sort_by {|t, w| w/@template_to_bucket[t].set.length}
            puts average_sort_templates.map {|k, v| v/@template_to_bucket[k].set.length}.inspect
            if average_sort_templates[0][0] == to_template
              to_template = average_sort_templates[1][0]
              puts 'd'
            else
              to_template = average_sort_templates[0][0]
              puts 'e'
            end
            #moved_shards.add(shard_to_move)
          end

          @template_to_bucket[sorted_templates[-1]].delete(shard_to_move)
          @template_to_bucket[to_template].add(shard_to_move)
          template_weights[sorted_templates[-1]] -= weight
          template_weights[to_template] += weight
          moved_shards.add(shard_to_move)
          puts "move"
        end
        sorted_templates = ordered_templates(template_weights)
        puts "(#{template_weights[sorted_templates[0]]} < #{lower} or #{template_weights[sorted_templates[-1]]} > #{upper}) and #{moved_shards.length} < #{@shards.length}"
      end
      print_template_weights(template_weights)
      puts "dead templates:"
      print_template_weights(dead_templates)
      puts "end dead templates"
    end

    def print_template_weights(template_weights)
      ordered = ordered_templates(template_weights)
      ordered.each {|t| puts "#{template_weights[t]}:\t#{t}"}
    end

    def ordered_shards(shards)
      shards.sort_by {|shard| -get_shard_weight(shard)}
    end

    def ordered_templates(template_weights)
      template_weights.keys.sort_by {|template| template_weights[template]}
    end

    def ordered_buckets(bucket_weights)
      bucket_weights.keys.sort_by {|bucket| bucket_weights[bucket]}
    end

    def memoized_concrete_descendants(t)
      @memoized_concrete_descendants ||= {}
      @memoized_concrete_descendants[t] ||= t.concrete_descendants
    end

    def transformations
      return @transformations if @transformations

      home!
      case @strategy 
      when "sticky" then rebalance_sticky_greedy!
      when "greedy" then rebalance_greedy!
      when "minimal" then rebalance_minimal!
      when "minagg" then rebalance_minimal_aggressive!
      end

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
  end
end
