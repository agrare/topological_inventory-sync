require "manageiq-messaging"
require "topological_inventory/sync/logging"

module TopologicalInventory
  class Sync
    class Worker
      include Logging

      def initialize(messaging_host, messaging_port)
        self.messaging_host = messaging_host
        self.messaging_port = messaging_port
      end

      def run
        logger.info("Starting Topological Inventory Sync Worker for #{queue_name}...")

        messaging_client = ManageIQ::Messaging::Client.open(messaging_client_opts)
        messaging_client.subscribe_topic(subscribe_opts) { |message| process_message(message) }
      ensure
        messaging_client&.close
      end

      private

      attr_accessor :messaging_host, :messaging_port, :queue_name

      def process_message(message)
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end

      def queue_name
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end

      def persist_ref
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end

      def messaging_client_opts
        {
          :protocol   => :Kafka,
          :host       => messaging_host,
          :port       => messaging_port,
          :group_ref  => "topological-inventory-sync-#{queue_name}",
          :client_ref => "topological-inventory-sync-#{queue_name}"
        }
      end

      def subscribe_opts
        {
          :service     => queue_name,
          :persist_ref => persist_ref
        }
      end
    end
  end
end
