require "active_support/core_ext/class/subclasses"

require "topological_inventory/sync/logging"
require "topological_inventory/sync/sources_sync_worker"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/version"

module TopologicalInventory
  class Sync
    include Logging

    def initialize(messaging_host, messaging_port)
      self.messaging_host   = messaging_host
      self.messaging_port   = messaging_port
      self.worker_classes   = Worker.subclasses
      self.threads          = {}
    end

    def run
      loop do
        ensure_threads
        sleep(10)
      end
    end

    private

    attr_accessor :messaging_host, :messaging_port, :threads, :worker_classes

    def ensure_threads
      worker_classes.each { |worker_class| ensure_thread(worker_class) }
    end

    def ensure_thread(worker_class)
      thread_id = worker_class.to_s
      return if threads[thread_id] && threads[thread_id].alive?
      threads[thread_id] = start_thread(worker_class)
    end

    def start_thread(worker_class)
      Thread.new do
        worker = worker_class.new(messaging_host, messaging_port)
        worker.run
      rescue => err
        logger.error(err)
        logger.error(err.backtrace.join("\n"))
      end
    end
  end
end
