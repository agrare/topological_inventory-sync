require_relative '../../../helpers/inventory_upload_helper'
require "sources-api-client"
require "topological_inventory/sync"
require "topological_inventory/sync/inventory_upload/processor_worker"

RSpec.describe TopologicalInventory::Sync::InventoryUpload::ProcessorWorker do
  include InventoryUploadHelper

  context "#perform" do
    let(:processor) do
      described_class.new("localhost", "9092")
    end
    let(:message) { ManageIQ::Messaging::ReceivedMessage.new(nil, nil, payload, nil, nil) }
    let(:request_id) { "52df9f748eabcfeb" }
    let(:file_path) { "/tmp/uploads/insights-upload-perm-test/#{request_id}" }
    let(:payload) { insights_upload_payload }
    let(:inventory) { ocp_inventory }

    context "with missing Source Type" do
      before do
        expect(TopologicalInventory::Sync::InventoryUpload::Parser)
          .to receive(:open_url).and_yield(targz(inventory))

        source_type_collection = double
        allow(source_type_collection).to receive(:data).and_return(source_type)

        allow_any_instance_of(SourcesApiClient::DefaultApi).to receive(:list_source_types).and_return(source_type_collection)
      end

      let(:source_type) { [] }

      it "raises exception when Source Type not found" do
        expect { processor.send(:perform, message) }.to raise_exception("Source Type #{inventory['source_type']} not found!")
      end
    end

    context "calling find_or_create_source" do
      let(:sources_api) { double }
      let(:source_data) { {:name => inventory['name'], :uid => inventory['source']} }
      let(:source) { double }


      it "returns existing Source" do
        allow(sources_api).to receive(:list_sources).and_return(source)
        allow(source).to receive(:data).and_return([source_data])

        response = processor.send(:find_or_create_source, sources_api, 1, inventory['name'], inventory['source'])

        expect(response).to eq(source_data)
      end

      context "when Source is not found" do
        it "creates new Source" do
          allow(sources_api).to receive(:list_sources).and_return(source)
          allow(source).to receive(:data).and_return([])

          allow(sources_api).to receive(:create_source_with_http_info).and_return([source_data, 201, nil])

          response = processor.send(:find_or_create_source, sources_api, '1', inventory['name'], inventory['source'])

          expect(response).to eq(source_data)
        end

        it "raises error when sources-api doesn't response with HTTP 201" do
          allow(sources_api).to receive(:list_sources).and_return(source)
          allow(source).to receive(:data).and_return([])

          allow(sources_api).to receive(:create_source_with_http_info).and_return([source_data, 400, nil])

          expect { processor.send(:find_or_create_source,
                                  sources_api,
                                  '1',
                                  inventory['name'],
                                  inventory['source'])}.to raise_exception("Failed to create Source #{ocp_inventory['name']} (#{ocp_inventory['source']})")
        end
      end
    end
  end
end
