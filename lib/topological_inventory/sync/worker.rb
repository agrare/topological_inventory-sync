require "manageiq-messaging"
require "faktory"
require "topological_inventory/sync/logging"

module TopologicalInventory
  class Sync
    class Worker
      include Logging

      def initialize(messaging_host, messaging_port, queue_name)
        self.faktory_client   = Faktory::Client.new
        self.messaging_host   = messaging_host
        self.messaging_port   = messaging_port
        self.queue_name       = queue_name
      end

      def run
        logger.info("Starting Topological Inventory Sync Worker for #{queue_name}...")

        messaging_client = ManageIQ::Messaging::Client.open(messaging_client_opts)
        messaging_client.subscribe_topic(subscribe_opts) { |message| process_message(message) }
      rescue => err
        logger.error(err.message)
        logger.error(err.backtrace.join("\n"))
      ensure
        messaging_client&.close
      end

      private

      attr_accessor :faktory_client, :messaging_host, :messaging_port, :queue_name

      def process_message(message)
        faktory_client.push(
          "jid"     => SecureRandom.hex(12),
          "queue"   => "default",
          "jobtype" => queue_name,
          "args"    => [
            message.message,
            message.payload
          ]
        )
      rescue => err
        logger.error(err.message)
        logger.error(err.backtrace.join("\n"))
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
          :persist_ref => "topological-inventory-sync"
        }
      end
    end
  end
end
