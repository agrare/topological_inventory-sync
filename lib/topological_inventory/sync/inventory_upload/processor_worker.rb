require "json"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/inventory_upload/parser"
require "pry-byebug"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class ProcessorWorker < Worker
        include Logging

        def worker_name
          "Topological Inventory Insights Upload Processor Worker"
        end

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

          inventory = TopologicalInventory::Sync::InventoryUpload::Parser.parse_inventory_payload(payload['url'])

          process_inventory(inventory, account)
        end

        private

        # @param inventory [Hash]
        # @param account [String] account from x-rh-identity header
        def process_inventory(inventory, account)
          sources_api = sources_api_client(account)

          # Find Source Type
          source_type = find_source_type(inventory['source_type'], sources_api)
          # Create source with first payload
          find_or_create_source(sources_api, source_type.id, inventory['name'], inventory['source'])
        end

        def find_source_type(source_type_name, sources_api)
          return if source_type_name.nil?

          response = sources_api.list_source_types({:filter => {:name => source_type_name}})
          if response.data.blank?
            raise "Source Type #{source_type_name} not found!"
          else
            logger.info("Source Type #{source_type_name} found")
            response.data.first
          end
        end

        def find_or_create_source(sources_api, source_type_id, source_name, source_uid)
          return if source_name.nil?

          sources = sources_api.list_sources({:filter => {:uid => source_uid}})

          if sources.data.blank?
            source = SourcesApiClient::Source.new(:uid => source_uid, :name => source_name, :source_type_id => source_type_id)
            source = sources_api.create_source(source)

            logger.info("Source #{source_name}(#{source_uid}) created successfully")
            source
          else
            logger.debug("Source #{source_name}(#{source_uid}) found")
            sources.data.first
          end
        rescue => e
          logger.error("Failed to get or create Source: #{source_uid} - #{e.message}")
          raise
        end
      end
    end
  end
end
