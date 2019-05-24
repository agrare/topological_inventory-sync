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
        payload = JSON.parse(message.payload)

        logger.info("#{payload}")

        validation = valid_payload?(payload) ? "success" : "failure"

        payload["validation"] = validation

        messaging_client.publish_topic(
          :service => "platform.upload.validation",
          :event   => "", # TODO we shouldn't require this in MIQ-Messaging
          :payload => payload.to_json
        )
      end

      private

      def valid_payload?(payload)
        schema_name = payload.dig("metadata", "schema", "name")
        schema_klass = schema_klass_name(schema_name).safe_constantize

        schema_klass.present?
      end

      def schema_klass_name(name)
        "TopologicalInventory::Schema::#{name}"
      end
    end
  end
end
