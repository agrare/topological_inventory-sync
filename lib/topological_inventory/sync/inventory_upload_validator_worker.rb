require "json"
require "topological_inventory/sync/worker"

module TopologicalInventory
  class Sync
    class InventoryUploadValidatorWorker < Worker
      include Logging

      def queue_name
        "platform.upload.topological-inventory"
      end

      def persist_ref
        "topological-inventory-upload-validator"
      end

      def perform(message)
        jobtype = message.message
        payload = JSON.parse(message.payload)

        logger.info("#{jobtype}: #{payload}")

        payload["validation"] = "success"

        messaging_client.publish_topic(
          :service => "platform.upload.validation",
          :event   => "", # TODO we shouldn't require this in MIQ-Messaging
          :payload => payload.to_json
        )
      end
    end
  end
end
