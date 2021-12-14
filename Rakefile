require 'rake/testtask'
require 'bundler'
require 'bundler/gem_tasks'

Bundler::GemHelper.install_tasks

Rake::TestTask.new('unit') do |t|
  t.libs << 'lib'
  t.libs << 'spec'
  t.test_files = FileList['spec/unit/**/*_spec.rb']
  t.verbose = true
end

Rake::TestTask.new('integration') do |t|
  t.libs << 'lib'
  t.libs << 'spec'
  t.test_files = FileList['spec/integration/**/*_spec.rb']
  t.verbose = true
end

Rake::TestTask.new('dev') do |t|
  t.libs << 'lib'
  t.libs << 'spec'

  files = FileList.new('spec/test_helper.rb')
  files.add(ENV["SINGLE_TEST"]) if ENV["SINGLE_TEST"]
  t.test_files = files

  t.verbose = true
end

task :specs => [:unit, :integration]
task :default => :specs
