#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
class HelpNeededError < RuntimeError; end
require "optparse"
require "ostruct"
require "gizzard"
require "yaml"

DOC_STRINGS = {
  "create" => "Create shard(s) of a given Java/Scala class.  If you don't know the list of available classes, you can just try a bogus class, and the exception will include a list of valid classes.",
  "wrap" => "Wrapping creates a new (virtual, e.g. blocking, replicating, etc.) shard, and relinks SHARD_ID_TO_WRAP's parent links to run through the new shard."
}

ORIGINAL_ARGV = ARGV.dup
zero = File.basename($0)

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

def split(string)
  return [] unless string
  a = []
  tokens = string.split(/\s+/)
  a << tokens.shift
  tokens.each do |token|
    s = a.last
    if s.length + token.length + 1 < 80
      s << " #{token}"
    else
      a << token
    end
  end
  a
end

def separators(opts, string)
  opts.separator("")
  split(string).each do |substr|
    opts.separator(substr)
  end
  opts.separator("")
end

subcommands = {
  'create' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} create [options] CLASS_NAME SHARD_ID [MORE SHARD_IDS...]"
    separators(opts, DOC_STRINGS["create"])
    
    opts.on("-s", "--source-type=TYPE") do |s|
      subcommand_options.source_type = s
    end

    opts.on("-d", "--destination-type=TYPE") do |s|
      subcommand_options.destination_type = s
    end
  end,
  'wrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} wrap CLASS_NAME SHARD_ID_TO_WRAP [MORE SHARD_IDS...]"
    separators(opts, DOC_STRINGS["wrap"])
  end,
  'subtree' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} subtree SHARD_ID"
    separators(opts, DOC_STRINGS["subtree"])
  end,
  'delete' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} delete SHARD_ID_TO_DELETE [MORE SHARD_IDS]"
    separators(opts, DOC_STRINGS["delete"])
  end,
  'addforwarding' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} addforwarding TABLE_ID BASE_ID SHARD_ID"
    separators(opts, DOC_STRINGS["addforwarding"])
  end,
  'forwardings' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} show [options]"
    separators(opts, DOC_STRINGS["forwardings"])

    opts.on("-t", "--tables=IDS", "Show only the specified table ids (comma separated)") do |table_ids|
      subcommand_options.table_ids ||= []
      subcommand_options.table_ids +=  table_ids.split(",").map { |s| s.to_i }
    end
  end,
  'unwrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} unwrap SHARD_ID_TO_REMOVE [MORE SHARD_IDS]"
    separators(opts, DOC_STRINGS["unwrap"])
  end,
  'find' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} find [options]"
    separators(opts, DOC_STRINGS["find"])

    opts.on("-t", "--type=TYPE", "Return only shards of the specified TYPE") do |shard_type|
      subcommand_options.shard_type = shard_type
    end

    opts.on("-H", "--host=HOST", "HOST of shard") do |shard_host|
      subcommand_options.shard_host = shard_host
    end
  end,
  'links' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} links SHARD_ID [MORE SHARD_IDS...]"
    separators(opts, DOC_STRINGS["links"])
  end,
  'info' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} info SHARD_ID [MORE SHARD_IDS...]"
    separators(opts, DOC_STRINGS["info"])
  end,
  'reload' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} reload"
    separators(opts, DOC_STRINGS["reload"])
  end,
  'addlink' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} link PARENT_SHARD_ID CHILD_SHARD_ID WEIGHT"
    separators(opts, DOC_STRINGS["addlink"])
  end,
  'unlink' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} unlink PARENT_SHARD_ID CHILD_SHARD_ID"
    separators(opts, DOC_STRINGS["unlink"])
  end,
  'lookup' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} lookup TABLE_ID SOURCE_ID"
    separators(opts, DOC_STRINGS["lookup"])
  end,
  'copy' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} copy SOURCE_SHARD_ID DESTINATION_SHARD_ID"
    separators(opts, DOC_STRINGS["copy"])
  end,
  'busy' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} busy"
    separators(opts, DOC_STRINGS["busy"])
  end,
  'setup-migrate' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} setup-migrate SOURCE_SHARD_ID DESTINATION_SHARD_ID"
    separators(opts, DOC_STRINGS["setup-migrate"])
  end,
  'finish-migrate' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} finish-migrate SOURCE_SHARD_ID DESTINATION_SHARD_ID"
    separators(opts, DOC_STRINGS["finish-migrate"])
  end,
  'inject' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} inject PRIORITY JOBS..."
    separators(opts, DOC_STRINGS["inject"])
  end
}

global = OptionParser.new do |opts|
  opts.banner = "Usage: #{zero} [global-options] SUBCOMMAND [subcommand-options]"
  opts.separator ""
  opts.separator "Gizzmo is a tool for manipulating the forwardings and replication structure of"
  opts.separator "Gizzard-based datastores.  It can also perform bulk job operations."
  opts.separator ""
  opts.separator "You can type `#{zero} help SUBCOMMAND` for help on a specific subcommand. It's"
  opts.separator "also useful to remember that global options come *before* the subcommand, while"
  opts.separator "subcommand options come *after* the subcommand."
  opts.separator ""
  opts.separator "You may find it useful to create a ~/.gizzmorc file, which is simply YAML"
  opts.separator "key/value pairs corresponding to options you want by default. A common .gizzmorc"
  opts.separator "simply contain:"
  opts.separator ""
  opts.separator "    host: localhost"
  opts.separator "    port: 7917"
  opts.separator ""
  opts.separator "Subcommands:"
  subcommands.keys.compact.sort.each do |sc|
    base = "  #{sc}"
    if doc = DOC_STRINGS[sc]
      base += " " * (20 - base.length)
      base += " -- "
      base += doc[0..(76 - base.length)]
      base += "..."
    end
    opts.separator base
  end
  opts.separator ""
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

log = global_options.log || "./gizzmo.log"

while !$stdin.tty? && line = STDIN.gets
  argv << line.strip
end

begin
  Gizzard::Command.run(subcommand_name, global_options, argv, subcommand_options, log)
rescue HelpNeededError => e
  if e.class.name != e.message
    STDERR.puts("=" * 80)
    STDERR.puts e.message
    STDERR.puts("=" * 80)
  end
  STDERR.puts subcommands[subcommand_name]
  exit 1
rescue ThriftClient::Simple::ThriftException, Gizzard::Thrift::ShardException, Errno::ECONNREFUSED => e
  STDERR.puts e.message
  exit 1
rescue Errno::EPIPE
  # This is just us trying to puts into a closed stdout.  For example, if you pipe into
  # head -1, then this script will keep running after head closes.  We don't care, and
  # seeing the backtrace is annoying!
rescue Interrupt
  exit 1
end