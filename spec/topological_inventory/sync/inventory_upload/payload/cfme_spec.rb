require "topological_inventory/sync/inventory_upload/payload"
require_relative "../../../../helpers/inventory_upload_helper"
require 'topological_inventory-api-client'

RSpec.describe TopologicalInventory::Sync::InventoryUpload::Payload::Cfme do
  include InventoryUploadHelper

  context "#process" do
    let(:source) { SourcesApiClient::Source.new }
    let(:application) { SourcesApiClient::Application.new }

    it "parses cfme inventory and sends to ingress-api" do
      payload = described_class.new(cfme_inventory, "12345")

      payload.send(:cfme_ems_types).each do |ems_type|
        cfme_inventory[ems_type].to_a.each do |provider_payload|
          expect(payload).to receive(:source_exists_in_topology_inventory?).with(provider_payload['guid']).and_return(true)
        end
      end

      expect(payload).to receive(:find_or_create_source).and_return(source)
      expect(payload).to receive(:find_or_create_application).and_return(application)
      expect(payload).to receive(:send_to_ingress_api)

      payload.process
    end

    it 'parses cfme inventory and sends to ingress-api when source exists in topological inventory' do
      payload = described_class.new(cfme_inventory, '12345')
      api_client = payload.send(:topological_api_client)

      payload.send(:cfme_ems_types).each do |ems_type|
        cfme_inventory[ems_type].to_a.each do |provider_payload|
          allow(payload).to receive(:find_source).with(api_client, provider_payload['guid']).and_return(source)
        end
      end

      expect(payload).to receive(:find_or_create_source).and_return(source)
      expect(payload).to receive(:find_or_create_application).and_return(application)
      expect(payload).to receive(:send_to_ingress_api)

      payload.process
    end

    it "doesn't send inventory to ingress-api when source doesn't exists in topological inventory yet" do
      described_class::TIMEOUT_COUNT = 0.001.seconds.freeze
      payload = described_class.new(cfme_inventory, '12345')
      api_client = payload.send(:topological_api_client)

      payload.send(:cfme_ems_types).each do |ems_type|
        cfme_inventory[ems_type].to_a.each do |provider_payload|
          guid = provider_payload['guid']
          allow(payload).to receive(:find_source).with(api_client, guid).and_return(nil)
        end
      end

      expect(payload).to receive(:find_or_create_source).and_return(source)
      expect(payload).to receive(:find_or_create_application).and_return(application)
      expect(payload).not_to receive(:send_to_ingress_api)

      payload.process
    end
  end
end
