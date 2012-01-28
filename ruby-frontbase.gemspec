Gem::Specification.new do |s|
  s.name = %q{ruby-frontbase}
  s.version = "1.0.1"
  s.summary = %q{FrontBase Ruby binding.}
  s.email = %q{mlaster@metavillage.com}
  s.homepage = %q{http://rubyforge.org/projects/ruby-frontbase}
  s.rubyforge_project = %q{ruby-frontbase}
  s.description = %q{Ruby bindings for the FrontBase database server (http://www.frontbase.com).}
  s.authors = ["Cail Borrell", "Mike Laster"]
  s.requirements = ["A C compiler.", "FrontBase 3.x"]
  s.files = ["README", "extconf.rb", "frontbase.c"]
  s.extensions = "extconf.rb"
  s.autorequire = 'frontbase'
end
