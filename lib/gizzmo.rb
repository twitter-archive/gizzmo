#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
class HelpNeededError < RuntimeError; end
require "optparse"
require "ostruct"
require "gizzard"
require "yaml"

ORIGINAL_ARGV = ARGV.dup

# Container for parsed options
global_options     = OpenStruct.new
global_options.render = []
subcommand_options = OpenStruct.new

# Leftover arguments
argv = nil

GIZZMO_VERSION = File.read(File.dirname(__FILE__) + "/../VERSION") rescue "unable to read version file"

begin
  YAML.load_file(File.join(ENV["HOME"], ".gizzmorc")).each do |k, v|
    global_options.send("#{k}=", v)
  end
rescue Errno::ENOENT
  # Do nothing...
rescue => e
  abort "Unknown error loading ~/.gizzmorc: #{e.message}"
end

subcommands = {
  'create' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} create [options] HOST TABLE_PREFIX CLASS_NAME"

    opts.on("-s", "--source-type=TYPE") do |s|
      subcommand_options.source_type = s
    end

    opts.on("-d", "--destination-type=TYPE") do |s|
      subcommand_options.destination_type = s
    end
  end,
  'wrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} wrap CLASS_NAME SHARD_ID_TO_WRAP [MORE SHARD_IDS...]"
  end,
  'subtree' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} subtree SHARD_ID"
  end,
  'delete' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} delete SHARD_ID_TO_DELETE [MORE SHARD_IDS]"
  end,
  'addforwarding' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} addforwarding TABLE_ID BASE_ID SHARD_ID"
  end,
  'forwardings' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} show [options]"

    opts.on("-t", "--tables=IDS", "Show only the specified table ids (comma separated)") do |table_ids|
      subcommand_options.table_ids ||= []
      subcommand_options.table_ids +=  table_ids.split(",").map { |s| s.to_i }
    end
  end,
  'unwrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} unwrap SHARD_ID_TO_REMOVE [MORE SHARD_IDS]"
  end,
  'find' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} find [options]"

    opts.on("-t", "--type=TYPE", "Return only shards of the specified TYPE") do |shard_type|
      subcommand_options.shard_type = shard_type
    end

    opts.on("-H", "--host=HOST", "HOST of shard") do |shard_host|
      subcommand_options.shard_host = shard_host
    end
  end,
  'links' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} links SHARD_ID [MORE SHARD_IDS...]"
  end,
  'info' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} info SHARD_ID [MORE SHARD_IDS...]"
  end,
  'reload' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} reload"
  end,
  'addlink' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} link PARENT_SHARD_ID CHILD_SHARD_ID WEIGHT"
  end,
  'unlink' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} unlink PARENT_SHARD_ID CHILD_SHARD_ID"
  end,
  'lookup' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} lookup TABLE_ID SOURCE_ID"
  end
}

global = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [global-options] SUBCOMMAND [subcommand-options]"
  opts.separator ""
  opts.separator "Subcommands:"
  subcommands.keys.compact.sort.each do |sc|
    opts.separator "  #{sc}"
  end
  opts.separator ""
  opts.separator "You can type `#{$0} help SUBCOMMAND` for help on a specific subcommand."
  opts.separator ""
  opts.separator "Global options:"

  opts.on("-H", "--host=HOSTNAME", "HOSTNAME of remote thrift service") do |host|
    global_options.host = host
  end

  opts.on("-P", "--port=PORT", "PORT of remote thrift service") do |port|
    global_options.port = port
  end
  
  opts.on("--subtree", "Render in subtree mode") do
    global_options.render << "subtree"
  end
  
  opts.on("--info", "Render in info mode") do
    global_options.render << "info"
  end

  opts.on("-D", "--dry-run", "") do |port|
    global_options.dry = true
  end

  opts.on("-C", "--config=YAML_FILE", "YAML_FILE of option key/values") do |file|
    YAML.load(File.open(file)).each do |k, v|
      global_options.send("#{k}=", v)
    end
  end

  opts.on("-L", "--log=LOG_FILE", "Path to LOG_FILE") do |file|
    global_options.log = file
  end

  opts.on("-f", "--force", "Don't display confirmation dialogs") do |force|
    global_options.force = force
  end
  
  opts.on_tail("-v", "--version", "Show version") do
    puts GIZZMO_VERSION
    exit
  end
end

# Print banner if no args
if ARGV.length == 0
  STDERR.puts global
  exit 1
end

# This
def process_nested_parsers(global, subcommands)
  begin
    global.order!(ARGV) do |subcommand_name|
      # puts args.inspect
      subcommand = subcommands[subcommand_name]
      argv = subcommand ? subcommand.parse!(ARGV) : ARGV
      return subcommand_name, argv
    end
  rescue => e
    STDERR.puts e.message
    exit 1
  end
end


subcommand_name, argv = process_nested_parsers(global, subcommands)

# Print help sub-banners
if subcommand_name == "help"
  STDERR.puts subcommands[argv.shift] || global
  exit 1
end

unless subcommands.include?(subcommand_name)
  STDERR.puts "Subcommand not found: #{subcommand_name}"
  exit 1
end

log = global_options.log || "/tmp/gizzmo.log"
service = Gizzard::Thrift::ShardManager.new(global_options.host, global_options.port, log, global_options.dry)

begin
  Gizzard::Command.run(subcommand_name, service, global_options, argv, subcommand_options)
rescue HelpNeededError => e
  if e.class.name != e.message
    STDERR.puts("=" * 80)
    STDERR.puts e.message
    STDERR.puts("=" * 80)
  end
  STDERR.puts subcommands[subcommand_name]
  exit 1
rescue ThriftClient::Simple::ThriftException => e
  STDERR.puts e.message
  exit 1
rescue Errno::EPIPE
  # This is just us trying to puts into a closed stdout.  For example, if you pipe into
  # head -1, then this script will keep running after head closes.  We don't care, and
  # seeing the backtrace is annoying!
rescue Interrupt
  exit 1
end