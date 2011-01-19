ROOT_DIR    = File.expand_path("../..", __FILE__)
TEST_ROOT   = File.expand_path("test", ROOT_DIR)
SERVER_ROOT = File.expand_path('test_server', TEST_ROOT)

SERVER_VERSION = '0.1'
SERVER_JAR     = File.expand_path("dist/gizzmotestserver/gizzmotestserver-#{SERVER_VERSION}.jar", SERVER_ROOT)

SERVICE_PORT = 7919
MANAGER_PORT = 7920
JOB_PORT     = 7921
SERVICE_DATABASE    = 'gizzard_test_integration'
NAMESERVER_DATABASE = 'gizzard_test_integration_ns'

class Object
  def T; p self; self end
end

class NilClass
  def T; p self; self end
end

require 'rubygems'
require 'spec'
require 'mysql'

$:.unshift File.expand_path('lib', ROOT_DIR)
require 'gizzard'

Spec::Runner.configure do |c|
  c.mock_with :rr
end

def id(h,p); Gizzard::ShardId.new(h,p) end
def info(h,p,c,s = "",d = "",b = 0); Gizzard::ShardInfo.new(id(h,p),c,s,d,b) end
def link(p,c,w); Gizzard::LinkInfo.new(p,c,w) end
def forwarding(t,b,s); Gizzard::Forwarding.new(t,b,s) end
def host(h,p,c,s = 0); Gizzard::Host.new(h,p,c,s) end

def mk_template(conf_tree)
  Gizzard::ShardTemplate.parse(conf_tree)
end

def test_server_pid
  if pid = `ps axo pid,command`.split("\n").find {|l| l[SERVER_JAR] }
    pid.split.first.to_i
  end
end

def start_test_server!(manager_p = MANAGER_PORT, job_p = JOB_PORT, service_p = SERVICE_PORT)
  unless test_server_pid
    compile_test_server!

    fork do
      exec "cd #{SERVER_ROOT} && exec java -jar #{SERVER_JAR} #{service_p} #{job_p} #{manager_p} > /dev/null 2>&1"
    end

    sleep 3
  end
end

def stop_test_server!
  if pid = test_server_pid
    Process.kill("KILL", pid)
  end
end

def compile_test_server!
  system "cd #{SERVER_ROOT} && sbt update package-dist" unless File.exist? SERVER_JAR
end

def mysql_connect!(host, user, pass)
  $mysql = Mysql.new(host, user, pass)
end

def drop_database(*ds)
  ds.each {|d| $mysql.query("drop database if exists `#{d}`") }
end

def create_database(*ds)
  ds.each {|d| $mysql.query("create database if not exists `#{d}`") }
end

def reset_nameserver(db = NAMESERVER_DATABASE)
  $mysql.query("delete from `#{db}`.shards")
  $mysql.query("delete from `#{db}`.shard_children")
  $mysql.query("delete from `#{db}`.forwardings")
  $mysql.query("delete from `#{db}`.hosts")

  nameserver.reload_config rescue nil
end

def reset_databases!
  drop_database SERVICE_DATABASE
  create_database NAMESERVER_DATABASE, SERVICE_DATABASE

  begin
    reset_nameserver
  rescue MysqlError

    begin
      nameserver.rebuild_schema
    rescue Errno::ECONNREFUSED
    end
  end
end

def read_nameserver_db(db = NAMESERVER_DATABASE)
  { :shards      => map_rs($mysql.query("select * from `#{db}`.shards"), &method(:as_shard)),
    :links       => map_rs($mysql.query("select * from `#{db}`.shard_children"), &method(:as_link)),
    :forwardings => map_rs($mysql.query("select * from `#{db}`.forwardings where deleted = 0"), &method(:as_forwarding)),
    :hosts       => map_rs($mysql.query("select * from `#{db}`.hosts"), &method(:as_host)) }
end

def map_rs(rs)
  a = []; rs.each_hash {|r| a << yield(r) }; a
end

def as_shard_id(h, prefix = nil)
  attrs = ['hostname', 'table_prefix'].map {|a| prefix ? [prefix, a].join('_') : a }
  Gizzard::ShardId.new(*h.values_at(*attrs))
end

def as_shard(h)
  attrs = h.values_at('class_name', 'source_type', 'destination_type') << h['busy'].to_i
  Gizzard::ShardInfo.new(as_shard_id(h), *attrs)
end

def as_link(h)
  Gizzard::LinkInfo.new(as_shard_id(h, 'parent'), as_shard_id(h, 'child'), h['weight'].to_i)
end

def as_forwarding(h)
  Gizzard::Forwarding.new(h['table_id'].to_i, h['base_id'].to_i, as_shard_id(h, 'shard'))
end

def as_host(h)
  Gizzard::Host.new(h['hostname'], h['port'].to_i, h['cluster'], h['status'].to_i)
end

def gizzmo(cmd)
  result = `cd #{ROOT_DIR} && ruby -rubygems -Ilib bin/gizzmo -H localhost -P #{MANAGER_PORT} #{cmd} 2>&1`
  puts result if ENV['GIZZMO_OUTPUT']
  result
end

def nameserver
  @nameserver ||= Gizzard::Nameserver.new('localhost:' + MANAGER_PORT.to_s)
end

alias ns nameserver

# setup

mysql_connect!("localhost", '', '')
reset_databases!

unless ENV['EXTERNAL_TEST_SERVER']
  start_test_server!
  at_exit { stop_test_server! }
end
