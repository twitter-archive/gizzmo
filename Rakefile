ROOT_DIR = File.expand_path(File.dirname(__FILE__))
begin
  require 'rubygems'
  require 'bundler/setup'
rescue LoadError
  $stderr.puts "bundler not found. run `gem install bundler`"
end

require 'rake'
require 'jeweler'
require 'spec/rake/spectask'
require 'rdoc/task'

task :default => :spec

Jeweler::Tasks.new do |gem|
  gem.name = "gizzmo"
  gem.summary = %Q{Gizzmo is a command-line client for managing gizzard clusters.}
  gem.description = %Q{Gizzmo is a command-line client for managing gizzard clusters.}
  gem.email = "stuhood@twitter.com"
  gem.homepage = "http://github.com/twitter/gizzmo"
  gem.authors = ["Kyle Maxwell"]
end
Jeweler::GemcutterTasks.new

Spec::Rake::SpecTask.new(:spec) do |t|
  spec_opts = File.expand_path('test/spec.opts', ROOT_DIR)
  if File.exist? spec_opts
    t.spec_opts = ['--options', "\"#{spec_opts}\""]
  end
  t.spec_files = FileList['test/**/*_spec.rb']
end

Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "gizzmo #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
