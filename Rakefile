require 'rubygems'
require 'rake'
require 'spec/rake/spectask'

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

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

Spec::Rake::SpecTask.new(:spec) do |t|
  spec_opts = File.expand_path('spec/spec.opts')
  if File.exist? spec_opts
    t.spec_opts = ['--options', "\"#{spec_opts}\""]
  end
  t.spec_files = FileList['spec/**/*_spec.rb']
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/test_*.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test do
  puts
  puts "=" * 79
  puts "You might want to read the README before running tests."
  puts "=" * 79
  sleep 2
  exec File.join(File.dirname(__FILE__), "test", "test.sh")
end

task :default => :test

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "gizzmo #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
