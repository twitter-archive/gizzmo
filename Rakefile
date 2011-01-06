ROOT_DIR = File.expand_path(File.dirname(__FILE__))
require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "gizzmo"
    gem.summary = %Q{Gizzmo is a command-line client for managing gizzard clusters.}
    gem.description = %Q{Gizzmo is a command-line client for managing gizzard clusters.}
    gem.email = "kmaxwell@twitter.com"
    gem.homepage = "http://github.com/twitter/gizzmo"
    gem.authors = ["Kyle Maxwell"]
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end


begin
  require 'spec/rake/spectask'
  Spec::Rake::SpecTask.new(:spec) do |t|
    spec_opts = File.expand_path('test/spec.opts', ROOT_DIR)
    if File.exist? spec_opts
      t.spec_opts = ['--options', "\"#{spec_opts}\""]
    end
    t.spec_files = FileList['test/**/*_spec.rb']
  end
rescue LoadError
  $stderr.puts "RSpec required to run tests."
end

task :test do
  puts
  puts "=" * 79
  puts "You might want to read the README before running tests."
  puts "=" * 79
  sleep 2
  exec File.join(File.dirname(__FILE__), "test", "test.sh")
end

task :default => :spec

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "gizzmo #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
