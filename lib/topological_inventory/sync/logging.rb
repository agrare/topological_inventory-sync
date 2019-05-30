require "manageiq/loggers"

module TopologicalInventory
  class Sync
    class << self
      attr_writer :logger
    end

    def self.logger
      @logger ||= ManageIQ::Loggers::CloudWatch.new
    end

    module Logging
      def logger
        TopologicalInventory::Sync.logger
      end
    end
  end
end
