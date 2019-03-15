require "topological_inventory/sync/logging"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/version"

module TopologicalInventory
  class Sync
    def initialize(messaging_host, messaging_port, messaging_queues)
      self.messaging_host   = messaging_host
      self.messaging_port   = messaging_port
      self.messaging_queues = messaging_queues
      self.threads          = {}
    end

    def run
      loop do
        ensure_threads
        sleep(10)
      end
    end

    private

    attr_accessor :messaging_host, :messaging_port, :messaging_queues, :threads

    def ensure_threads
      messaging_queues.each { |queue_name| ensure_thread(queue_name) }
    end

    def ensure_thread(queue_name)
      return if threads[queue_name] && threads[queue_name].alive?
      threads[queue_name] = start_thread(queue_name)
    end

    def start_thread(queue_name)
      Thread.new do
        worker = Worker.new(messaging_host, messaging_port, queue_name)
        worker.run
      end
    end
  end
end
