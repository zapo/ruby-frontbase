require 'rubygems'
require 'rake'
require 'rake/testtask'
require 'rake/rdoctask'
require 'rake/packagetask'
require 'rake/gempackagetask'

desc "Default Task"
task :default => [ :build_gem_file ]

desc 'Build gem file'
task :build_gem_file do 
  %x( gem build ruby-frontbase.gemspec )
end
