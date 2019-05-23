require "json"
require "topological_inventory/sync/worker"

module TopologicalInventory
  class Sync
    class InventoryUploadWorker < Worker
      include Logging

      def queue_name
        "platform.upload.available"
      end

      def persist_ref
        "topological-inventory-upload-processor"
      end

      def perform(message)
        payload = JSON.parse(message.payload)
        return unless payload["service"] == "topological-inventory"

        logger.info("#{payload}")
      end
    end
  end
end
