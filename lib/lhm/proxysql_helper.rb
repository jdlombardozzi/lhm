module Lhm
  module ProxySQLHelper
    extend self
    ANNOTATION = "/*maintenance:lhm*/"

    def tagged(sql)
      "#{ANNOTATION}#{sql}"
    end
  end
end