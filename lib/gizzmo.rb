#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
class HelpNeededError < RuntimeError; end
require "optparse"
require "ostruct"
require "gizzard"
require "yaml"

DOC_STRINGS = {
  "addforwarding" => "Add a forwarding from a graph_id / base_source_id to a given shard.",
  "addlink" => "Add a relationship link between two shards.",
  "create" => "Create shard(s) of a given Java/Scala class. If you don't know the list of available classes, you can just try a bogus class, and the exception will include a list of valid classes.",
  "drill" => "Show shard trees for replicas of a given structure signature (from 'report').",
  "dump" => "Show shard trees for given table ids.",
  "find" => "Show all shards with a given hostname.",
  "finish-replica" => "Remove the write-only barrier in front of a shard that's finished being copied after 'setup-replica'.",
  "flush" => "Flush error queue for a given priority.",
  "forwardings" => "Get a list of all forwardings.",
  "hosts" => "List hosts used in shard names in the forwarding table and replicas.",
  "info" => "Show id/class/busy for shards.",
  "inject" => "Inject jobs (as literal json) into the server. Jobs can be linefeed-terminated from stdin, or passed as arguments. Priority is server-defined, but typically lower numbers (like 1) are lower priority.",
  "links" => "List parent & child links for shards.",
  "lookup" => "Lookup the shard id that holds the record for a given table / source_id.",
  "markbusy" => "Mark a shard as busy.",
  "pair" => "Report the replica pairing structure for a list of hosts.",
  "reload" => "Instruct application servers to reload the nameserver state.",
  "repair-shard" => "Repair shard",
  "report" => "Show each unique replica structure for a given list of shards. Usually this shard list comes from << gizzmo forwardings | awk '{ print $3 }' >>.",
  "setup-replica" => "Add a replica to be parallel to an existing replica, in write-only mode, ready to be copied to.",
  "wrap" => "Wrapping creates a new (virtual, e.g. blocking, replicating, etc.) shard, and relinks SHARD_ID_TO_WRAP's parent links to run through the new shard.",
}

ORIGINAL_ARGV = ARGV.dup
zero = File.basename($0)

# Container for parsed options
global_options = OpenStruct.new
global_options.port          = 7920
global_options.injector_port = 7921
global_options.render        = []
global_options.framed        = false

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

def load_config(options, filename)
  YAML.load(File.open(filename)).each do |k, v|
    k = "hosts" if k == "host"
    v = v.split(",").map {|h| h.strip } if k == "hosts"
    options.send("#{k}=", v)
  end
end

def add_scheduler_opts(subcommand_options, opts)
  opts.on("--max-copies=COUNT", "Limit max simultaneous copies to COUNT.") do |c|
    (subcommand_options.scheduler_options ||= {})[:max_copies] = c.to_i
  end
  opts.on("--copies-per-host=COUNT", "Limit max copies per individual destination host to COUNT") do |c|
    (subcommand_options.scheduler_options ||= {})[:copies_per_host] = c.to_i
  end
  opts.on("--poll-interval=SECONDS", "Sleep SECONDS between polling for copy status") do |c|
    (subcommand_options.scheduler_options ||= {})[:poll_interval] = c.to_i
  end
  opts.on("--copy-wrapper=TYPE", "Wrap copy destination shards with TYPE. default WriteOnlyShard") do |t|
    (subcommand_options.scheduler_options ||= {})[:copy_wrapper] = t
  end
  opts.on("--no-progress", "Do not show progress bar at bottom.") do
    (subcommand_options.scheduler_options ||= {})[:no_progress] = true
  end
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
  'dump' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} dump TABLE_ID [TABLE_ID...]"
    separators(opts, DOC_STRINGS["dump"])
  end,
  'wrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} wrap CLASS_NAME SHARD_ID_TO_WRAP [MORE SHARD_IDS...]"
    separators(opts, DOC_STRINGS["wrap"])
  end,
  'pair' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} pair"
    separators(opts, DOC_STRINGS["pair"])
  end,
  'subtree' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} subtree SHARD_ID"
    separators(opts, DOC_STRINGS["subtree"])
  end,
  'markbusy' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} markbusy SHARD_ID"
    separators(opts, DOC_STRINGS["markbusy"])
  end,
  'markunbusy' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} markunbusy SHARD_ID"
    separators(opts, DOC_STRINGS["markunbusy"])
  end,
  'hosts' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} hosts"
    separators(opts, DOC_STRINGS["hosts"])
  end,
  'deleteforwarding' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} deleteforwarding TABLE_ID BASE_ID SHARD_ID"
    separators(opts, DOC_STRINGS["deleteforwarding"])
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
    opts.banner = "Usage: #{zero} forwardings [options]"
    separators(opts, DOC_STRINGS["forwardings"])

    opts.on("-t", "--tables=IDS", "Show only the specified table ids (comma separated)") do |table_ids|
      subcommand_options.table_ids ||= []
      subcommand_options.table_ids +=  table_ids.split(",").map { |s| s.to_i }
    end
    opts.on("-x", "--hex", "Show base ids in hex") do
      subcommand_options.hex = true
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

    opts.on("-h", "--shard-host=HOST", "HOST of shard") do |shard_host|
      subcommand_options.shard_host = shard_host
    end
  end,
  'links' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} links SHARD_ID [MORE SHARD_IDS...]"
    separators(opts, DOC_STRINGS["links"])

    opts.on("--ids", "Show shard ids only") do
      subcommand_options.ids = true
    end
    opts.on("--up", "Show uplinks only") do
      subcommand_options.up = true
    end
    opts.on("--down", "show downlinks only") do
      subcommand_options.down = true
    end
  end,
  'info' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} info SHARD_ID [MORE SHARD_IDS...]"
    separators(opts, DOC_STRINGS["info"])
  end,
  'reload' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} reload"
    separators(opts, DOC_STRINGS["reload"])
  end,
  'drill' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} drill SIGNATURE"
    separators(opts, DOC_STRINGS["drill"])
  end,
  'addlink' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} addlink PARENT_SHARD_ID CHILD_SHARD_ID WEIGHT"
    separators(opts, DOC_STRINGS["addlink"])
  end,
  'unlink' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} unlink PARENT_SHARD_ID CHILD_SHARD_ID"
    separators(opts, DOC_STRINGS["unlink"])
  end,

  'report' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} report [options]"
    separators(opts, DOC_STRINGS["report"])
    opts.on("--flat", "Show flat report") do
      subcommand_options.flat = true
    end
  end,

  'lookup' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} lookup [options] TABLE_ID SOURCE"
    separators(opts, DOC_STRINGS["lookup"])

    opts.on("--fnv", "Use FNV1A_64 hash on source") do
      subcommand_options.hash_function = :fnv
    end
  end,
  'copy' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} copy SOURCE_SHARD_ID DESTINATION_SHARD_ID"
    separators(opts, DOC_STRINGS["copy"])
  end,
  'repair-shard' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} repair-shard SOURCE_SHARD_ID DESTINATION_SHARD_ID TABLE_ID"
    separators(opts, DOC_STRINGS["repair-shard"])
  end,
  'busy' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} busy"
    separators(opts, DOC_STRINGS["busy"])
  end,
  'setup-replica' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} setup-replica SOURCE_SHARD_ID DESTINATION_SHARD_ID"
    separators(opts, DOC_STRINGS["setup-replica"])
  end,
  'finish-replica' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} finish-replica SOURCE_SHARD_ID DESTINATION_SHARD_ID"
    separators(opts, DOC_STRINGS["finish-replica"])
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
  end,
  'flush' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} flush --all|PRIORITY"
    separators(opts, DOC_STRINGS["flush"])

    opts.on("--all", "Flush all error queues.") do
      subcommand_options.flush_all = true
    end
  end,
  'add-host' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} add-host HOSTS"
    separators(opts, DOC_STRINGS["add-host"])
  end,
  'remove-host' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} remove-host HOST"
    separators(opts, DOC_STRINGS["remove-host"])
  end,
  'list-hosts' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} list-hosts"
    separators(opts, DOC_STRINGS["list-hosts"])
  end,
  'topology' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} topology [options]"
    separators(opts, DOC_STRINGS["topology"])

    opts.on("--forwardings", "Show topology by forwarding instead of counts") do
      subcommand_options.forwardings = true
    end

    opts.on("--shards", "Show topology by root shard ids instead of counts") do
      subcommand_options.root_shards = true
    end
  end,
  'transform-tree' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} transform-tree [options] ROOT_SHARD_ID TEMPLATE"
    separators(opts, DOC_STRINGS['transform-tree'])

    add_scheduler_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end,
  'transform' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} transform [options] FROM_TEMPLATE TO_TEMPLATE ..."
    separators(opts, DOC_STRINGS['transform'])

    add_scheduler_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end,
  'rebalance' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} rebalance [options] WEIGHT TO_TEMPLATE ..."
    separators(opts, DOC_STRINGS["rebalance"])

    add_scheduler_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end
}

rc_path = ENV['GIZZMORC'] || "#{ENV["HOME"]}/.gizzmorc"
if File.exists?(rc_path)
  load_config(global_options, rc_path)
end

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
  opts.separator "simply contains:"
  opts.separator ""
  opts.separator "    hosts: localhost"
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
  opts.on("-H", "--hosts=HOST[,HOST,...]", "HOSTS of application servers") do |hosts|
    global_options.hosts = hosts.split(",").map {|h| h.strip }
  end

  opts.on("-H", "--host=HOST", "HOST of application servers") do |hosts|
    global_options.hosts = hosts.split(",").map {|h| h.strip }
  end



  opts.on("-P", "--port=PORT", "PORT of remote manager service. default 7920") do |port|
    global_options.port = port.to_i
  end

  opts.on("-I", "--injector=PORT", "PORT of remote job injector service. default 7921") do |port|
    global_options.injector_port = port.to_i
  end

  opts.on("-T", "--tables=TABLE[,TABLE,...]", "TABLE ids of forwardings to affect") do |tables|
    global_options.tables = tables.split(",").map {|t| t.to_i }
  end

  opts.on("-F", "--framed", "use the thrift framed transport") do |framed|
    global_options.framed = true
  end

  opts.on("-r", "--retry=TIMES", "TIMES to retry the command") do |r|
    global_options.retry = r.to_i
  end

  opts.on("-t", "--timeout=SECONDS", "SECONDS to let the command run") do |r|
    global_options.timeout = r.to_i
  end

  opts.on("--subtree", "Render in subtree mode") do
    global_options.render << "subtree"
  end

  opts.on("--info", "Render in info mode") do
    global_options.render << "info"
  end

  opts.on("-D", "--dry-run", "") do
    global_options.dry = true
  end

  opts.on("-C", "--config=YAML_FILE", "YAML_FILE of option key/values") do |filename|
    load_config(global_options, filename)
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

def custom_timeout(seconds)
  if seconds
    begin
      require "rubygems"
      require "system_timer"
      SystemTimer.timeout_after(seconds) do
        yield
      end
    rescue LoadError
      require "timeout"
      Timeout.timeout(seconds) do
        yield
      end
    end
  else
    yield
  end
end

begin
  custom_timeout(global_options.timeout) do
    Gizzard::Command.run(subcommand_name, global_options, argv, subcommand_options, log)
  end
rescue HelpNeededError => e
  if e.class.name != e.message
    STDERR.puts("=" * 80)
    STDERR.puts e.message
    STDERR.puts("=" * 80)
  end
  STDERR.puts subcommands[subcommand_name]
  exit 1
rescue ThriftClient::Simple::ThriftException, Gizzard::GizzardException, Errno::ECONNREFUSED => e
  STDERR.puts e.message
  STDERR.puts e.backtrace
  exit 1
rescue Errno::EPIPE
  # This is just us trying to puts into a closed stdout.  For example, if you pipe into
  # head -1, then this script will keep running after head closes.  We don't care, and
  # seeing the backtrace is annoying!
rescue Interrupt
  exit 1
end
