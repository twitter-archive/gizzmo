$:.unshift File.expand_path('../../lib', __FILE__)

require 'rubygems'

# testing requires
require 'spec'
require 'rr'

Spec::Runner.configure do |config|
  config.mock_with :rr
end

# source requires
require 'gizzard'
require 'gizzard/nameserver'
require 'gizzard/transformation'
require 'gizzard/migrator'
require 'gizzard/shard_template'


def make_shard_template(conf_tree)
  config = Gizzard::MigratorConfig.new
  Gizzard::ShardTemplate.from_config(config, conf_tree)
end
