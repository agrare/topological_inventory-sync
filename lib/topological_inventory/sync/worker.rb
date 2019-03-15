require "manageiq-messaging"
require "topological_inventory/sync/logging"

module TopologicalInventory
  class Sync
    class Worker
      include Logging

      def initialize(messaging_host, messaging_port, queue_name)
        self.messaging_host   = messaging_host
        self.messaging_port   = messaging_port
        self.queue_name       = queue_name
      end

      def run
        logger.info("Starting Topological Inventory Sync Worker for #{queue_name}...")

        messaging_client = ManageIQ::Messaging::Client.open(messaging_client_opts)
        messaging_client.subscribe_messages(subscribe_opts) do |messages|
          messages.each { |message |process_message(message) }
        end
      rescue => err
        logger.error(err.message)
        logger.error(err.backtrace.join("\n"))
      ensure
        messaging_client&.close
      end

      private

      attr_accessor :messaging_host, :messaging_port, :queue_name

      def process_message(message)
      rescue => err
        logger.error(err.message)
        logger.error(err.backtrace.join("\n"))
      end

      def messaging_client_opts
        {
          :protocol => :Kafka,
          :host     => messaging_host,
          :port     => messaging_port
        }
      end

      def subscribe_opts
        {
          :service => queue_name,
        }
      end
    end
  end
end
