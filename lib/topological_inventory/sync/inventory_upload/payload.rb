require "sources-api-client"
require "topological_inventory/sync/inventory_upload/payload/cfme"
require "topological_inventory/sync/inventory_upload/payload/default"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class Payload
        include Logging

        class << self
          def load(message)
            account, url = message.values_at("account", "url")
            unpack(url) do |json|
              payload = payload_klass(json)&.new(json, account)
              yield payload if payload.present?
            end
          end

          def unpack(url)
            open_url(url) do |io|
              untargz(io) do |file|
                require "json/stream"
                yield JSON::Stream::Parser.parse(file)
              end
            end
          end

          private

          def payload_klass(json)
            "#{name}::#{json.dig("schema", "name")}".safe_constantize
          end

          def untargz(io)
            require "rubygems/package"
            Zlib::GzipReader.wrap(io) do |gz|
              Gem::Package::TarReader.new(gz) do |tar|
                tar.each { |entry| yield entry }
              end
            end
          end

          def open_url(url)
            require "http"

            uri = URI(url)
            if ["file", nil].include?(uri.scheme)
              File.open(uri.path) { |f| yield f }
            else
              response = HTTP.get(uri)
              response.body.stream!
              yield response.body
            end
          end
        end

        def initialize(payload, account)
          @account = account
          @payload = payload
        end

        private

        attr_reader :account, :payload

        def find_or_create_source(source_type_name, source_name, source_uid)
          sources_api = sources_api_client(account)
          source_type = find_source_type(source_type_name, sources_api)

          sources = sources_api.list_sources({:filter => {:uid => source_uid}})
          if sources.data.blank?
            source = SourcesApiClient::Source.new(:uid => source_uid, :name => source_name, :source_type_id => source_type.id)
            source, status_code, _ = sources_api.create_source_with_http_info(source)

            if status_code == 201
              logger.info("Source #{source_name}(#{source_uid}) created successfully")
              source
            else
              raise "Failed to create Source #{source_name} (#{source_uid})"
            end
          else
            logger.debug("Source #{source_name} (#{source_uid}) found")
            sources.data.first
          end
        end

        def find_source_type(source_type_name, sources_api)
          raise 'Missing Source Type name!' if source_type_name.blank?

          response = sources_api.list_source_types({:filter => {:name => source_type_name}})
          if response.data.blank?
            raise "Source Type #{source_type_name} not found!"
          else
            logger.info("Source Type #{source_type_name} found")
            response.data.first
          end
        end

        def send_to_ingress_api(inventory)
          logger.info("[START] Send to Ingress API with :refresh_state_uuid => '#{inventory.refresh_state_uuid}'...")

          sender = ingress_api_sender

          # Send data to ingress_api
          total_parts = sender.save(:inventory => inventory)

          # Send total parts sent to ingress_api
          sender.save(
            :inventory => inventory_for_sweep(inventory, total_parts)
          )

          logger.info("[COMPLETED] Send to Ingress API with :refresh_state_uuid => '#{inventory.refresh_state_uuid}'. Total parts: #{total_parts}")
          total_parts
        end

        def inventory_for_sweep(inventory, total_parts)
          TopologicalInventoryIngressApiClient::Inventory.new(
            :name => inventory.name,
            :schema => TopologicalInventoryIngressApiClient::Schema.new(:name => inventory.schema.name),
            :source => inventory.source,
            :collections => [],
            :refresh_state_uuid => inventory.refresh_state_uuid,
            :total_parts => total_parts,
            :sweep_scope => inventory.collections.collect { |collection| collection.name }.compact
          )
        end

        def ingress_api_client
          TopologicalInventoryIngressApiClient::DefaultApi.new
        end

        def ingress_api_sender
          TopologicalInventoryIngressApiClient::SaveInventory::Saver.new(
            :client => ingress_api_client,
            :logger => logger
          )
        end

        def sources_api_client(tenant = nil)
          api_client = SourcesApiClient::ApiClient.new

          if tenant
            api_client.default_headers.merge!(
              {
                "x-rh-identity" => Base64.strict_encode64(
                  JSON.dump({"identity" => {"account_number" => tenant}})
                )
              }
            )
          end

          SourcesApiClient::DefaultApi.new(api_client)
        end
      end
    end
  end
end
