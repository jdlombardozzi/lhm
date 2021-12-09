module Lhm
  module ProxySQLHelper
    extend self
    ANNOTATION = "/*maintenance:lhm*/"

    attr_writer :disable_tags

    # Default value
    def disable_tags
      return false unless defined?(@disable_tags)
      @disable_tags
    end

    def tagged(sql)
      "#{ANNOTATION + " " unless disable_tags}#{sql}"
    end
  end
end
