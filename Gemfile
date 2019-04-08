source "https://rubygems.org"

plugin "bundler-inject", "~> 1.1"
require File.join(Bundler::Plugin.index.load_paths("bundler-inject")[0], "bundler-inject") rescue nil

gem "activesupport",    "~> 5.2.2"
gem "manageiq-loggers", "~> 0.1.0"
gem "manageiq-messaging"
gem "optimist"

group :development, :test do
  gem "rake"
  gem "rspec"
  gem "simplecov"
  gem "webmock"
end
