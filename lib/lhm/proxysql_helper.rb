module Lhm
  module ProxySQLHelper
    extend self
    ANNOTATION = "/*maintenance:lhm*/"

    def tagged(sql)
      "#{sql} #{ANNOTATION}"
    end
  end
end
