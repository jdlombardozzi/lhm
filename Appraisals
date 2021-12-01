# Oldest supported Rails version
appraise "activerecord-5.2" do
  gem "activerecord", "5.2.6"

# This gemfile will be ran against ruby 2.3 and newer simplecov requires 2.5
  gem "simplecov", "0.17.1"
  gem "docile", "1.3.5"
end

# First conflicted version
appraise "activerecord-6.0" do
  gem "activerecord", "6.0.0"
end

# Second conflicted version
appraise "activerecord-6.1" do
  gem "activerecord", "6.1.0"
end

# Latest version at the moment
appraise "activerecord-7.0.0.alpha2" do
  gem "activerecord", "7.0.0.alpha2"
end