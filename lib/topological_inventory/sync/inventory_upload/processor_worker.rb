require "json"
require "topological_inventory/sync/worker"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class ProcessorWorker < Worker
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

          account, request_id, payload_id = payload.values_at("account", "request_id", "payload_id")
          log_header = "account [#{account}] request_id [#{request_id}]"

          logger.info("#{log_header}: Processing payload [#{payload_id}]...")
        end
      end
    end
  end
end
