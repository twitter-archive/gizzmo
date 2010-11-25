ROOT_DIR    = File.expand_path("../..", __FILE__)
TEST_ROOT   = File.expand_path("test", ROOT_DIR)
SERVER_ROOT = File.expand_path('test_server', TEST_ROOT)

SERVER_VERSION = '0.1'
SERVER_JAR     = File.expand_path("dist/gizzmotestserver/gizzmotestserver-#{SERVER_VERSION}.jar", SERVER_ROOT)

SERVICE_PORT = 7919
MANAGER_PORT = 7920
JOB_PORT     = 7921
SERVICE_DATABASE    = 'gizzmo_test_integration'
NAMESERVER_DATABASE = 'gizzmo_test_integration_ns'


require 'rubygems'
require 'spec'
require 'mysql'
require 'open3'

$:.unshift File.expand_path('lib', ROOT_DIR)
require 'gizzard'

Spec::Runner.configure do |c|
  c.mock_with :rr
end

def test_server_pid
  if pid = `ps axo pid,command`.split("\n").find {|l| l[SERVER_JAR] }
    pid.split.first.to_i
  end
end

def start_test_server!(manager_p = MANAGER_PORT, job_p = JOB_PORT, service_p = SERVICE_PORT)
  unless test_server_pid
    fork do
      exec("cd #{SERVER_ROOT} && exec java -jar #{SERVER_JAR} #{service_p} #{job_p} #{manager_p} > /dev/null 2>&1")
    end

    sleep(3)
  end
end

def stop_test_server!
  if pid = test_server_pid
    Process.kill("KILL", pid)
  end
end

def compile_test_server!
  system "cd #{ROOT} && sbt package-dist" unless File.exist? SERVER_JAR
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
end

def reset_databases!
  drop_database SERVICE_DATABASE
  create_database NAMESERVER_DATABASE, SERVICE_DATABASE

  begin
    reset_nameserver
  rescue MysqlError

    begin
      m = Gizzard::Thrift::Manager.new("localhost", MANAGER_PORT, '/dev/null')
      m.rebuild_schema
    rescue Errno::ECONNREFUSED
    end
  end
end

def read_nameserver_db(db = NAMESERVER_DATABASE)
  { :shards      => map_rs($mysql.query("select * from `#{db}`.shards"), &method(:as_shard)),
    :links       => map_rs($mysql.query("select * from `#{db}`.shard_children"), &method(:as_link)),
    :forwardings => map_rs($mysql.query("select * from `#{db}`.forwardings"), &method(:as_forwarding)),
    :hosts       => map_rs($mysql.query("select * from `#{db}`.hosts"), &method(:as_host)) }
end

def map_rs(rs)
  a = []; rs.each_hash {|r| a << yield(r) }; a
end

def as_shard_id(h, prefix = nil)
  attrs = ['hostname', 'table_prefix'].map {|a| prefix ? [prefix, a].join('_') : a }
  Gizzard::Thrift::ShardId.new(*h.values_at(*attrs))
end

def as_shard(h)
  attrs = h.values_at('class_name', 'source_type', 'destination_type') << h['busy'].to_i
  Gizzard::Thrift::ShardInfo.new(as_shard_id(h), *attrs)
end

def as_link(h)
  Gizzard::Thrift::LinkInfo.new(as_shard_id(h, 'parent'), as_shard_id(h, 'child'), h['weight'].to_i)
end

def as_forwarding(h)
  Gizzard::Thrift::Forwarding.new(h['table_id'].to_i, h['base_id'].to_i, as_shard_id(h, 'shard'))
end

def as_host(h)
  Gizzard::Thrift::Host.new(h['hostname'], h['port'].to_i, h['cluster'], h['status'].to_i)
end


# setup

mysql_connect!("localhost", '', '')
reset_databases!
start_test_server!

at_exit { stop_test_server! }
