#!/usr/bin/env ruby

$: << "/Users/robey/twitter/gizzmo/lib"
require 'rubygems'
require 'yaml'
require 'ostruct'
require 'gizzard'

#require 'migrator'
#require 'transformation'


def usage(parser, reason)
  puts
  puts "ERROR: #{reason}"
  puts
  puts parser
  puts
  exit 1
end

options = {
  :config_filename => ENV['HOME'] + "/.topology.yml",
  :dry_run => false,
  :max_copies => 20,
  :table_id => 0,
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [options]"
  opts.separator "Example: #{$0} -f topology.yml"

  opts.on("-f", "--config=FILENAME", "load database topology config (default: #{options[:config_filename]})") do |filename|
    options[:config_filename] = filename
  end
  opts.on("-i", "--id=TABLE_ID", "table id to use (default: #{options[:table_id]})") do |table_id|
    options[:table_id] = table_id.to_i
  end
  opts.on("-s", "--shards=N", "create N shards") do |n|
    options[:total_shards] = n.to_i
  end
  opts.on("-t", "--table=TABLE_NAME", "table name") do |table_name|
    options[:table_name] = table_name
  end
  opts.on("-c", "--class=CLASS_NAME", "shard class to create") do |class_name|
    options[:shard_class] = class_name
  end
  opts.on("-n", "--dry-run", "don't actually send gizzard commands") do
    options[:dry_run] = true
  end
  opts.on("-x", "--max=COPIES", "max concurrent copies (default: #{options[:max_copies]})") do |n|
    options[:max_copies] = n.to_i
  end
end

parser.parse!(ARGV)

config = begin
  YAML.load_file(options[:config_filename])
rescue => e
  puts "Exception while reading config file: #{e}"
  {}
end

total_shards = options[:total_shards] || config['total_shards']
usage(parser, "Must define total_shards or -s") unless total_shards

table_name = options[:table_name] || config['table_name']
usage(parser, "Must define table_name or -t") unless table_name

migrator_config = Gizzard::MigratorConfig.new
migrator_config.namespace = config['namespace'] || nil
migrator_config.table_prefix = table_name
migrator_config.table_id = options[:table_id]
migrator_config.source_type = config['source_type'] || ''
migrator_config.destination_type = config['destination_type'] || ''

querying_nameserver = Gizzard::Nameserver.new(config['app_hosts'] || 'localhost', :dry_run => false)
mutating_nameserver = Gizzard::Nameserver.new(config['app_hosts'] || 'localhost', :dry_run => options[:dry_run])

usage(parser, "Config file must contain a 'partitions' definition") unless config['partitions']

# Where the magic happens:

new_templates = config['partitions'].map { |p| Gizzard::ShardTemplate.from_config(p) }

EXISTING_CONFIG_CACHE = File.expand_path(".existing_shards.yml")

if File.exist? EXISTING_CONFIG_CACHE
  puts "Loading cache from previous operation. If this is not desired, delete the cache file at #{EXISTING_CONFIG_CACHE}"
  existing_config = YAML.load_file(EXISTING_CONFIG_CACHE)

  existing_map = {}

  existing_config["partitions"].each_with_index do |tree, i|
    shards = existing_config["shards"][i]
    existing_map[Gizzard::ShardTemplate.from_config(tree)] = shards
  end
else
  puts "Querying nameserver..."
  existing_map = Gizzard::ShardTemplate.existing_template_map(querying_nameserver)

  existing_config = { 'partitions' => [], 'shards' => [] }

  existing_map.each do |template, shards|
    existing_config['partitions'] << template.to_config
    existing_config['shards'] << shards
  end

  File.open(EXISTING_CONFIG_CACHE, 'w') { |f| f.write YAML.dump(existing_config) }
end






puts "\nCalculating changes...\n"


migrator = Gizzard::Migrator.new(existing_map, new_templates, total_shards, migrator_config, :ordered)

puts "\nUNCHANGED:"
pp migrator.unchanged_templates

puts "\nSIMILAR:"
pp migrator.similar_templates

puts "\nNEW:"
pp migrator.new_templates

puts "\nUNRECOGNIZED:"
pp migrator.unrecognized_templates

unless (transformations = migrator.transformations).empty?
  puts "\nTRANSFORMATIONS:"
  transformations.each { |t| puts ""; p t }

  printf "Apply migration? [yN] "; $stdout.flush

  if $stdin.gets.chomp == 'y'
    pages = transformations.inject([]) { |pages, t| pages.concat t.paginate(options[:max_copies]) }

    pages.each_with_index do |page, page_idx|
      puts "\nTRANSFORMATION #{page_idx + 1} / #{pages.length}:"

      puts page.inspect(true)

      t_start = Time.now

      if page.must_copy?
        puts "    Preparing nameserver for copies."
        page.prepare! mutating_nameserver
        mutating_nameserver.reload_forwardings

        puts "    Scheduling copies."
        page.copy! mutating_nameserver

        puts "    Waiting for copies to finish."
        page.wait_for_copies mutating_nameserver

        puts "    Finalizing nameserver changes."
        page.cleanup! mutating_nameserver
        mutating_nameserver.reload_forwardings
      else
        puts "   Applying nameserver changes."
        page.apply! mutating_nameserver
      end

      puts "--- Time Elapsed: %0.3f ---" % (Time.now - t_start).to_f
    end

    mutating_nameserver.reload_forwardings

    puts "Done!"

  else
    puts "Aborting migration."
  end

else
  puts "No migration needed."
end

# remove the existing config state since we're done, and the system is
# in a known good state.
File.unlink EXISTING_CONFIG_CACHE
