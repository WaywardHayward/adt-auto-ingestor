using System;
using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.Ingestion.Face;
using Azure;
using Azure.DigitalTwins.Core;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.OPC
{
    public class OpcMessageIngestor : AbstractMessageIngestor, IMessageIngestor
    {
        public OpcMessageIngestor(IngestionContext context) : base(context)
        {
            context.Log.LogInformation($"Processing OPC Message");
        }

        public async Task Ingest(EventData eventData, JObject message)
        {
            var twinId = message.SelectToken("NodeId", false);

            if (twinId == null)
                return;

            await IngestOpcItem(message, await EnsureModelExists(message));
        }
        protected override Task<string> EnsureModelExists(JObject message)
        {
            return EnsureModelExists(message, $"dtmi:com:microsoft:autoingest:opcnode", "OPC Node");
        }

        protected override async Task<string> EnsureModelExists(JObject message, string rawModelId, string name)
        {
            var modelId = $"{rawModelId};1";
            var models = await GetAllModels();
            var modelExists = models.Any(m => m.Id.Split(";")[0] == rawModelId);

            if (modelExists)
                return modelId;

            var opcNodeModel = new DigitalTwinModel
            {
                Id = modelId,
                DisplayName = $"OPC Node Auto Provisioned Model"
            };

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "NodeId",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "DisplayName",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "Value",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "SourceTimestamp",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

            var modelDtdl = JsonConvert.SerializeObject(opcNodeModel, Formatting.Indented);
            _context.Log.LogInformation($"Creating Auto Model {modelId} in {_context.AdtUrl}\n: {modelDtdl}");

            var newModels = await _context.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });

            if (newModels.Value.Any())
                _context.Log.LogInformation($"Created Auto Model {modelId} in {_context.AdtUrl}");

            return modelId;
        }

        private async Task IngestOpcItem(JObject message, string twinId)
        {
            await WriteToTwin(message, twinId, await EnsureModelExists(message));
        }

        protected override async Task WriteToTwin(JObject message, string twinId, string modelId)
        {
            var twin = await GetTwin(twinId);

            if (twin == null)
                await CreateOpcNodeTwin(message, modelId, twinId);
            else
                await UpdateOpcNodeTwin(message, twin, twinId, modelId);
        }

        private async Task UpdateOpcNodeTwin(JObject message, BasicDigitalTwin twin, string twinId, string modelId)
        {
            var patch = new JsonPatchDocument();

            patch.AppendReplace("/$metadata/$model", modelId);

            var value = message.SelectToken("Value", false) ?? message.SelectToken("Value.Value");
            var timestamp = message.SelectToken("Value", false) ?? message.SelectToken("Value.SourceTimestamp");
            var nodeId = twinId;
            var displayName = message.SelectToken("DisplayName");

            if (value!= null)
                AddPropertyPatch(patch, twin, "Value", value.ToString());

            if (displayName!= null)
                AddPropertyPatch(patch, twin,"DisplayName", displayName.ToString());

            if (timestamp!= null)
                AddPropertyPatch(patch, twin,"Timestamp", timestamp.ToString());

            await _context.DigitalTwinsClient.UpdateDigitalTwinAsync(twinId, patch, twin.ETag);
        }

        private async Task CreateOpcNodeTwin(JObject message, string modelId, string twinId)
        {
            var twin = new BasicDigitalTwin()
            {
                Id = twinId,
                Metadata = new DigitalTwinMetadata
                {
                    ModelId = modelId
                }
            };


            var value = message.SelectToken("Value", false) ?? message.SelectToken("Value.Value");
            var timestamp = message.SelectToken("Value", false) ?? message.SelectToken("Value.SourceTimestamp");
            var nodeId = twinId;
            var displayName = message.SelectToken("DisplayName");

            if (value!= null)
                twin.Contents.Add("Value", value.ToString());

            if (displayName!= null)
                twin.Contents.Add("DisplayName", displayName.ToString());

            if (timestamp!= null)
                twin.Contents.Add("Timestamp", timestamp.ToString());

            _context.Log.LogInformation($"Updating or Creating Twin {twinId} in {_context.AdtUrl}");

            await _context.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, twin);
        }
    }
}