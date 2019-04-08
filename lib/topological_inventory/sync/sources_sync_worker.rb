require "topological_inventory/sync/worker"

module TopologicalInventory
  class Sync
    class SourcesSyncWorker < Worker
      include Logging

      def process_message(message)
        logger.info("Got it! #{message}")
      end

      def queue_name
        "platform.sources.event-stream"
      end

      def persist_ref
        "topological-inventory-sync-sources"
      end
    end
  end
end
