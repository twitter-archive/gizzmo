#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
class HelpNeededError < RuntimeError; end
require "optparse"
require "ostruct"
require "gizzard"
require "shellwords"
require "yaml"

DOC_STRINGS = {
  "add-host" => "Add a remote cluster host to replicate to. Format: cluster:host:port.",
  "addforwarding" => "Add a forwarding from a graph_id / base_source_id to a given shard.",
  "addlink" => "Add a relationship link between two shards.",
  "add-partition" => "Rebalance the cluster by appending new partitions to the current topology.",
  "busy" => "List any shards with a busy flag set.",
  "copy" => "Copy between the given list of shards. Given a set of shards, it will copy and repair to ensure that all shards have the latest data.",
  "create" => "Create shard(s) of a given Java/Scala class. If you don't know the list of available classes, you can just try a bogus class, and the exception will include a list of valid classes.",
  "create-table" => "Create tables in an existing cluster.",
  "delete" => "", # TODO: Undocumented
  "deleteforwarding" => "", # TODO: Undocumented
  "diff-shards" => "Log differences between n shards",
  "drill" => "Show shard trees for replicas of a given structure signature (from 'report').",
  "dump" => "Show shard trees for given table ids.",
  "find" => "Show all shards with a given hostname.",
  "finish-migrate" => "", # TODO: Undocumented
  "finish-replica" => "Remove the write-only barrier in front of a shard that's finished being copied after 'setup-replica'.",
  "flush" => "Flush error queue for a given priority.",
  "forwardings" => "Get a list of all forwardings.",
  "hosts" => "List hosts used in shard names in the forwarding table and replicas.",
  "info" => "Show id/class/busy for shards.",
  "inject" => "Inject jobs (as literal json) into the server. Jobs can be linefeed-terminated from stdin, or passed as arguments. Priority is server-defined, but typically lower numbers (like 1) are lower priority.",
  "links" => "List parent and child links for shards.",
  "list-hosts" => "List remote cluster hosts being replicated to.",
  "lookup" => "Lookup the shard id that holds the record for a given table / source_id.",
  "markbusy" => "Mark a list of shards as busy.",
  "markunbusy" => "Mark a list of shards as not busy.",
  "pair" => "Report the replica pairing structure for a list of hosts.",
  "rebalance" => "Restructure and move shards to reflect a new list of tree structures.",
  "reload" => "Instruct application servers to reload the nameserver state.",
  "remove-host" => "Remove a remote cluster host being replicate to.",
  "remove-partition" => "Rebalance the cluster by removing the provided partitions from the current topology.",
  "repair-tables" => "Reconcile all the shards in the given tables (supplied with -T) by detecting differences and writing them back to shards as needed.",
  "report" => "Show each unique replica structure for a given list of shards. Usually this shard list comes from << gizzmo forwardings | awk '{ print $3 }' >>.",
  "setup-migrate" => "", # TODO: Undocumented
  "setup-replica" => "Add a replica to be parallel to an existing replica, in write-only mode, ready to be copied to.",
  "subtree" => "Show the subtree of replicas given a shard id.",
  "tables" => "List the table IDs known by this nameserver.",
  "topology" => "List the full topologies known for the table IDs provided.",
  "transform" => "Transform from one topology to another.",
  "transform-tree" => "Transforms given forwardings to the corresponding given tree structure.",
  "unlink" => "Remove a link from one shard to another.",
  "unwrap" => "Remove a wrapper created with wrap.",
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
    #global_options.send("#{k}=", v)
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
    if k == "template_options"
      opts = {}
      v.each do |k1, v1|
        opts[k1.to_sym] = v1
      end
      v = opts
    end
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
  opts.on("--copy-wrapper=SHARD_TYPE", "Wrap copy destination shards with SHARD_TYPE. default BlockedShard") do |t|
    (subcommand_options.scheduler_options ||= {})[:copy_wrapper] = t
  end
  opts.on("--skip-copies", "Do transformation without copying. WARNING: This is VERY DANGEROUS if you don't know what you're doing!") do
    (subcommand_options.scheduler_options ||= {})[:skip_copies] = true
  end
  opts.on("--no-progress", "Do not show progress bar at bottom.") do
    (subcommand_options.scheduler_options ||= {})[:no_progress] = true
  end
  opts.on("--batch-finish", "Wait until all copies are complete before cleaning up unneeded links and shards") do
    (subcommand_options.scheduler_options ||= {})[:batch_finish] = true
  end
end

def add_template_opts(subcommand_options, opts)
  opts.on("--virtual=SHARD_TYPE", "Concrete shards will exist behind a virtual shard of this SHARD_TYPE (default ReplicatingShard)") do |t|
    (subcommand_options.template_options ||= {})[:replicating] = t
  end

  opts.on("-c", "--concrete=SHARD_TYPE", "Concrete shards will be this SHARD_TYPE (REQUIRED when using --simple)") do |t|
    (subcommand_options.template_options ||= {})[:concrete] = t
  end

  opts.on("--source-type=DATA_TYPE", "The data type for the source column. (REQUIRED when using --simple)") do |t|
    (subcommand_options.template_options ||= {})[:source_type] = t
  end

  opts.on("--dest-type=DATA_TYPE", "The data type for the destination column. (REQUIRED when using --simple)") do |t|
    (subcommand_options.template_options ||= {})[:dest_type] = t
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
  'tables' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} tables"
    separators(opts, DOC_STRINGS["tables"])
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
    opts.banner = "Usage: #{zero} copy SHARD_IDS..."
    separators(opts, DOC_STRINGS["copy"])
  end,
  'repair-tables' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} -T TABLE,... repair-tables [options]"
    separators(opts, DOC_STRINGS["repair-tables"])
    opts.on("--max-copies=COUNT", "Limit max simultaneous copies to COUNT. (default 100)") do |c|
      subcommand_options.num_copies = c.to_i
    end
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
    opts.banner = "Usage: #{zero} transform-tree [options] TEMPLATE ROOT_SHARD_ID ..."
    separators(opts, DOC_STRINGS['transform-tree'])

    add_scheduler_opts subcommand_options, opts
    add_template_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end,
  'transform' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} transform [options] FROM_TEMPLATE TO_TEMPLATE ..."
    separators(opts, DOC_STRINGS['transform'])

    add_scheduler_opts subcommand_options, opts
    add_template_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end,
  'rebalance' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} rebalance [options] WEIGHT TO_TEMPLATE ..."
    separators(opts, DOC_STRINGS["rebalance"])

    add_scheduler_opts subcommand_options, opts
    add_template_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end,
  'add-partition' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} add-partition [options] TEMPLATE ..."
    separators(opts, DOC_STRINGS["add-partition"])
    add_template_opts subcommand_options, opts

    add_scheduler_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end,
  'remove-partition' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} remove-partition [options] TEMPLATE ..."
    separators(opts, DOC_STRINGS["remove-partition"])
    add_template_opts subcommand_options, opts

    add_scheduler_opts subcommand_options, opts

    opts.on("-q", "--quiet", "Do not display transformation info (only valid with --force)") do
      subcommand_options.quiet = true
    end
  end,
  'create-table' => OptionParser.new do |opts|
    opts.banner = "Usage: #{zero} create-table [options] WEIGHT TEMPLATE ..."
    separators(opts, DOC_STRINGS["create-table"])

    add_template_opts subcommand_options, opts

    opts.on("--shards=COUNT", "Create COUNT shards for each table.") do |count|
      subcommand_options.shards = count.to_i
    end

    opts.on("--min-id=NUM", "Set lower bound on the id space to NUM (default 0)") do |min_id|
      subcommand_options.min_id = min_id.to_i
    end

    opts.on("--max-id=NUM", "Set upper bound on the id space to NUM (default 2^60 - 1)") do |max_id|
      subcommand_options.max_id = max_id.to_i
    end

    opts.on("--base-name=NAME", "Use NAME as the base prefix for each shard's table prefix (default 'shard')") do |base_name|
      subcommand_options.base_name = base_name
    end

    opts.on("-q", "--quiet", "Do not display table creation info (only valid with --force)") do
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
  opts.separator "You can find explanations and example usage on the wiki (go/gizzmo)."
  opts.separator ""
  opts.separator "You may find it useful to create a ~/.gizzmorc file, which is simply YAML"
  opts.separator "key/value pairs corresponding to options you want by default. A common .gizzmorc"
  opts.separator "simply contains:"
  opts.separator ""
  opts.separator "    hosts: localhost"
  opts.separator "    port: 7920"
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
  opts.on("-H", "--hosts=HOST[,HOST,...]", "Comma-delimited list of application servers") do |hosts|
    global_options.hosts = hosts.split(",").map {|h| h.strip }
  end

  opts.on("-P", "--port=PORT", "PORT of remote manager service (default 7920)") do |port|
    global_options.port = port.to_i
  end

  opts.on("-I", "--injector=PORT", "PORT of remote job injector service (default 7921)") do |port|
    global_options.injector_port = port.to_i
  end

  opts.on("-T", "--tables=TABLE[,TABLE,...]", "TABLE ids of forwardings to affect") do |tables|
    global_options.tables = tables.split(",").map {|t| t.to_i }
  end

  opts.on("-F", "--framed", "Use the thrift framed transport") do |framed|
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

  opts.on("-s", "--simple", "Represent shard templates in a simple format") do
    (global_options.template_options ||= {})[:simple] = true #This is a temporary setting until the nameserver design changes match the simpler format
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
 
  opts.on("--argv=FILE", "Put the contents of FILE onto the command line") do |f|
    ARGV.push *Shellwords.shellwords(File.read(f))
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
    Gizzard::ShardTemplate.configure((global_options.template_options || {}).merge(subcommand_options.template_options || {}))
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
  STDERR.puts "\nERROR: Received an unhandled interrupt"
  exit 1
end
