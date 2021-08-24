// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
            string twinId = ExtractTwinId(message);

            if (twinId == null)
                return;

            await IngestOpcItem(message, twinId.ToString());
        }

        private string ExtractTwinId(JObject message)
        {
            var twinId = message.SelectToken("NodeId", false) ?? message.SelectToken("Id", false);
            var applicationUri = message.SelectToken("ApplicationUri", false)?.ToString();

            twinId = twinId.ToString().Replace(";",string.Empty).Replace(" ",string.Empty).Trim();

            if (!string.IsNullOrEmpty(applicationUri))
                twinId = $"{applicationUri.Split(":")[1]}/{twinId}";

            return twinId?.ToString();
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
                Name = "ApplicationUri",
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

            patch.AppendAdd("/$metadata/$model", modelId);

            var value =  message.SelectToken("Value.Value", false) ?? message.SelectToken("Value", false);
            var timestamp =  message.SelectToken("Value.SourceTimestamp") ?? message.SelectToken("SourceTimestamp", false);
            var nodeId = message.SelectToken("NodeId", false);
            var applicationUri = message.SelectToken("ApplicationUri", false);
            var displayName = message.SelectToken("DisplayName");

            if (nodeId != null)
                AddPropertyPatch(patch, "NodeId", nodeId.ToString());

            if (value != null)
                AddPropertyPatch(patch, "Value", value.ToString());

            if (displayName != null)
                AddPropertyPatch(patch, "DisplayName", displayName.ToString());

            if (timestamp != null)
                AddPropertyPatch(patch, "SourceTimestamp", timestamp.ToString());


            if (applicationUri != null)
                AddPropertyPatch(patch, "ApplicationUri", applicationUri.ToString());

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


            var value = message.SelectToken("Value.Value", false)  ?? message.SelectToken("Value", false) ;
            var timestamp = message.SelectToken("Value.SourceTimestamp") ?? message.SelectToken("SourceTimestamp", false);
            var applicationUri = message.SelectToken("ApplicationUri", false);
            var nodeId = message.SelectToken("NodeId", false);
            var displayName = message.SelectToken("DisplayName");

            if (nodeId != null)
                twin.Contents.Add("NodeId", nodeId.ToString());

            if (value != null)
                twin.Contents.Add("Value", value.ToString());

            if (displayName != null)
                twin.Contents.Add("DisplayName", displayName.ToString());

            if (timestamp != null)
                twin.Contents.Add("SourceTimestamp", timestamp.ToString());

            if (applicationUri != null)
                twin.Contents.Add("ApplicationUri", applicationUri.ToString());

            _context.Log.LogInformation($"Updating or Creating Twin {twinId} in {_context.AdtUrl}");

            await _context.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, twin);
        }
    }
}