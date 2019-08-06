module TopologicalInventory
  class Sync
    module InventoryUpload
      class Payload
        class Cfme < Payload
          def process
            cfme_ems_types.each do |ems_type|
              payload[ems_type].to_a.each do |ems_payload|
                inventory = process_cfme_provider_inventory(ems_type, ems_payload)
                send_to_ingress_api(inventory)
              end
            end
          end

          private

          def process_cfme_provider_inventory(ems_type, ems_payload)
            source_type = ems_type_to_source_type(ems_type)
            source_uid  = ems_payload["guid"]
            source_name = ems_payload["name"]

            find_or_create_source(source_type, source_name, source_uid)

            inventory = TopologicalInventoryIngressApiClient::Inventory.new(
              :source                  => source_uid,
              :source_type             => source_type,
              :schema                  => TopologicalInventoryIngressApiClient::Schema.new(:name => "Default"),
              :name                    => source_name,
              :refresh_state_uuid      => SecureRandom.uuid,
              :refresh_state_part_uuid => SecureRandom.uuid,
              :collections             => [],
            )

            if ems_payload["ems_clusters"].present?
              clusters_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "clusters", :data => [])
              ems_payload["ems_clusters"].each do |cluster_data|
                clusters_collection.data << TopologicalInventoryIngressApiClient::Cluster.new(
                  :name       => cluster_data["name"],
                  :source_ref => cluster_data["ems_ref"],
                  :uid_ems    => cluster_data["uid_ems"],
                  :extra      => {
                    :ha_enabled       => cluster_data["ha_enabled"],
                    :drs_enabled      => cluster_data["drs_enabled"],
                    :effective_cpu    => cluster_data["effective_cpu"],
                    :effective_memory => cluster_data["effective_memory"]
                  }
                )
              end
              inventory.collections << clusters_collection
            end

            if ems_payload["hosts"].present?
              hosts_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "hosts", :data => [])

              ems_payload["hosts"].each do |host_data|
                memory_mb = host_data.dig("hardware", "memory_mb")
                cluster_ref = host_data.dig("ems_cluster", "ems_ref")
                cluster     = TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                  :inventory_collection_name => "clusters", :reference => {:source_ref => cluster_ref}, :ref => :manager_ref
                ) unless cluster_ref.nil?

                hosts_collection.data << TopologicalInventoryIngressApiClient::Host.new(
                  :name        => host_data["name"],
                  :hostname    => host_data["hostname"],
                  :ipaddress   => host_data["ipaddress"],
                  :power_state => host_data["power_state"],
                  :uid_ems     => host_data["uid_ems"],
                  :source_ref  => host_data["ems_ref"],
                  :cpus        => host_data["cpu_total_cores"],
                  :memory      => memory_mb * 1048576,
                  :cluster     => cluster,
                  :extra       => {
                    :cpu_cores_per_socket => host_data["cpu_cores_per_socket"],
                    :maintenance          => host_data["maintenance"],
                    :vmm_vendor           => host_data["vmm_vendor"],
                    :vmm_version          => host_data["vmm_version"],
                    :vmm_product          => host_data["vmm_product"],
                    :vmm_buildnumber      => host_data["vmm_buildnumber"],
                  }
                )
              end

              inventory.collections << hosts_collection
            end

            if ems_payload["storages"].present?
              datastores_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "datastores", :data => [])
              datastore_mounts_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "datastore_mounts", :data => [])

              ems_payload["storages"].each do |storage_data|
                datastore_data = {
                  :name        => storage_data["name"],
                  :location    => storage_data["location"],
                  :total_space => storage_data["total_space"],
                  :free_space  => storage_data["free_space"],
                  :extra       => {
                    :uncommitted         => storage_data["uncommitted"],
                    :storage_domain_type => storage_data["storage_domain_type"]
                  }
                }

                storage_data["host_storages"].group_by { |hs| hs["ems_ref"] }.each do |ems_ref, host_storages|
                  datastores_collection.data << TopologicalInventoryIngressApiClient::Datastore.new(
                    datastore_data.merge(:source_ref => ems_ref)
                  )

                  host_storages.each do |host_storage|
                    host_ref = host_storage.dig("host", "ems_ref")
                    next if host_ref.nil?

                    datastore_mounts_collection.data << TopologicalInventoryIngressApiClient::DatastoreMount.new(
                      :datastore => TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                        :inventory_collection_name => "datastores", :reference => {:source_ref => ems_ref}, :ref => :manager_ref
                      ),
                      :host      => TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                        :inventory_collection_name => "hosts", :reference => {:source_ref => host_ref}, :ref => :manager_ref
                      ),
                    )
                  end
                end
              end

              inventory.collections << datastores_collection
              inventory.collections << datastore_mounts_collection
            end

            if ems_payload["vms"].present?
              vms_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "vms", :data => [])
              ems_payload["vms"].each do |vm_data|
                host_ref = vm_data.dig("host", "ems_ref")
                host     = TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                  :inventory_collection_name => "hosts", :reference => {:source_ref => host_ref}, :ref => :manager_ref
                ) unless host_ref.nil?

                vms_collection.data << TopologicalInventoryIngressApiClient::Vm.new(
                  :name        => vm_data["name"],
                  :description => vm_data["description"],
                  :cpus        => vm_data["cpu_total_cores"],
                  :memory      => vm_data["ram_size_in_bytes"],
                  :source_ref  => vm_data["ems_ref"],
                  :uid_ems     => vm_data["uid_ems"],
                  :power_state => vm_data["power_state"],
                  :host        => host,
                  :extra       => {
                    :cpu_cores_per_socket => vm_data["cpu_cores_per_socket"],
                    :disks_aligned        => vm_data["disks_aligned"],
                    :has_rdm_disk         => vm_data["has_rdm_disk"],
                    :linked_clone         => vm_data["linked_clone"],
                    :retired              => vm_data["retired"],
                    :v_datastore_path     => vm_data["v_datastore_path"],
                  }
                )
              end
              inventory.collections << vms_collection
            end

            inventory
          end

          def ems_type_to_source_type(ems_type)
            case ems_type
            when "ManageIQ::Providers::OpenStack::CloudManager"
              "openstack"
            when "ManageIQ::Providers::Redhat::InfraManager"
              "rhv"
            when "ManageIQ::Providers::Vmware::InfraManager"
              "vsphere"
            else
              raise "Invalid provider type #{ems_type}"
            end
          end

          def cfme_ems_types
            [
              "ManageIQ::Providers::OpenStack::CloudManager",
              "ManageIQ::Providers::Redhat::InfraManager",
              "ManageIQ::Providers::Vmware::InfraManager"
            ]
          end
        end
      end
    end
  end
end
