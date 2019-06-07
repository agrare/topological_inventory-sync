require "topological_inventory/sync/inventory_upload/validator_worker"

RSpec.describe TopologicalInventory::Sync::InventoryUpload::ValidatorWorker do
  context "#perform" do
    let(:validator) { described_class.new("localhost", "9092") }
    let(:message)         { ManageIQ::Messaging::ReceivedMessage.new(nil, nil, payload, nil, nil) }
    let(:request_id) { "52df9f748eabcfeb" }
    let(:file_path) { "/tmp/uploads/insights-upload-perm-test/#{request_id}" }
    let(:payload) do
      "{\"account\":\"12345\",\"rh_account\":\"12345\",\"principal\":\"54321\",\"request_id\":\"52df9f748eabcfeb\",\
      \"payload_id\":\"52df9f748eabcfeb\",\"size\":458,\"service\":\"topological-inventory\",\"category\":\"something\",\
      \"b64_identity\":\"eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0=\",\
      \"url\":\"/tmp/upload/schema.tar.gz\"}"
    end

    before do
      expect(TopologicalInventory::Sync::InventoryUpload::Parser)
        .to receive(:open_url).and_yield(targz(inventory))
    end

    context "with a valid inventory payload" do
      let(:inventory) do
        {"name"=>"OCP", "schema"=>{"name"=>"Default"}, "source"=>"90df364e-b82b-4617-91c5-98c3f3518544", "collections"=>[]}
      end

      it "publishes a valid payload" do
        expect(validator).to receive(:publish_validation).with(hash_including("validation" => "success"))
        validator.send(:perform, message)
      end
    end

    context "with an invalid inventory payload" do
      let(:inventory) do
        {"name"=>"OCP", "schema"=>{"name"=>"ManageIQ"}, "source"=>"90df364e-b82b-4617-91c5-98c3f3518544", "collections"=>[]}
      end

      it "publishes an invalid payload" do
        expect(validator).to receive(:publish_validation).with(hash_including("validation" => "failure"))
        validator.send(:perform, message)
      end
    end

    def targz(payload)
      require "rubygems/package"

      file = StringIO.new("", "w")
      Zlib::GzipWriter.wrap(file) do |gz|
        Gem::Package::TarWriter.new(gz) do |tar|
          payload_json = payload.to_json
          tar.add_file_simple("inventory.json", 0444, payload_json.length) do |io|
            io.write(payload_json)
          end
        end
      end

      StringIO.new(file.string, "r")
    end
  end
end
