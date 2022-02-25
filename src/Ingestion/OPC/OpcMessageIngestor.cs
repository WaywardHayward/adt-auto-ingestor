// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.Helpers;
using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingestor.AzureDigitalTwins;
using Azure;
using Azure.DigitalTwins.Core;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.OPC
{
    public class OpcMessageIngestor : AbstractMessageIngestor, IMessageIngestor
    {
        public OpcMessageIngestor(ILogger<OpcMessageIngestor> log, DigitalTwinModelCache modelCache, OpcMessageTwinIdProvider idProvider, IConfiguration configuration, DigitalTwinCache twinCache) : base(modelCache, idProvider,log, configuration, twinCache)
        {

        }

        public async Task Ingest(MessageContext context)
        {
            try
            {
                string twinId = ExtractTwinId(context.Message);

                if (twinId == null)
                    return;

                await IngestOpcItem(context, twinId.ToString());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error Ingesting OPC Message {context.Message.ToString()}");
            }
        }

        private string ExtractTwinId(JObject message)
        {
            var twinId = message.SelectToken("NodeId", false) ?? message.SelectToken("Id", false);
            var applicationUri = message.SelectToken("ApplicationUri", false)?.ToString();

            twinId = twinId.ToString().Replace(";", string.Empty).Replace(" ", string.Empty).Trim();

            if (!string.IsNullOrEmpty(applicationUri))
                twinId = $"{applicationUri.Split(":")[1]}/{twinId}";

            return twinId?.ToString();
        }

        protected override Task<string> EnsureModelExists(MessageContext context)
        {
            return EnsureModelExists(context, $"dtmi:com:microsoft:autoingest:opcnode", "OPC Node");
        }

        protected override string GetSourceTimestamp(MessageContext context)
        {
            return context.Message.SelectDateTimeTokenString("SourceTimestamp") ?? context.Message.SelectDateTimeTokenString("Timestamp") ?? DateTime.UtcNow.ToString();
        }

        protected override async Task<string> EnsureModelExists(MessageContext context, string rawModelId, string name)
        {
            var modelId = $"{rawModelId};1";
            var models = await GetAllModels(rawModelId);
            var model =  models.OrderByDescending(m => m.ModelVersion.Value).FirstOrDefault();

            if (model != null)
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
            _logger.LogInformation($"Creating Auto Model {modelId} in {context.IngestionContext.AdtUrl}\n: {modelDtdl}");

            var newModels = await context.IngestionContext.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });

            if (newModels.Value.Any())
                _logger.LogInformation($"Created Auto Model {modelId} in {context.IngestionContext.AdtUrl}");

            return modelId;
        }

        private async Task IngestOpcItem(MessageContext context, string twinId)
        {
            await WriteToTwin(context, twinId, await EnsureModelExists(context));
        }

        protected override async Task WriteToTwin(MessageContext context, string twinId, string modelId)
        {
            var twin = await GetTwin(context, twinId);

            if (twin == null)
                await CreateOpcNodeTwin(context, modelId, twinId);
            else
                await UpdateOpcNodeTwin(context, twin, twinId, modelId);
        }

        private async Task UpdateOpcNodeTwin(MessageContext context, BasicDigitalTwin twin, string twinId, string modelId)
        {
            var patch = new JsonPatchDocument();

            patch.AppendAdd("/$metadata/$model", modelId);

            var value = context.Message.SelectToken("Value.Value", false) ?? context.Message.SelectToken("Value", false);
            var timestamp = context.Message.SelectToken("Value.SourceTimestamp") ?? context.Message.SelectToken("SourceTimestamp", false);
            var nodeId = context.Message.SelectToken("NodeId", false);
            var applicationUri = context.Message.SelectToken("ApplicationUri", false);
            var displayName = context.Message.SelectToken("DisplayName");

            if (nodeId != null)
                AddPropertyPatch(patch, "NodeId", nodeId.ToString(), context);

            if (value != null)
                AddPropertyPatch(patch, "Value", value.ToString(), context);

            if (displayName != null)
                AddPropertyPatch(patch, "DisplayName", displayName.ToString(), context);

            if (timestamp != null)
                AddPropertyPatch(patch, "SourceTimestamp", timestamp.ToString(), context);


            if (applicationUri != null)
                AddPropertyPatch(patch, "ApplicationUri", applicationUri.ToString(), context);

            await context.IngestionContext.DigitalTwinsClient.UpdateDigitalTwinAsync(twinId, patch, twin.ETag);
        }

        private async Task CreateOpcNodeTwin(MessageContext context, string modelId, string twinId)
        {
            var twin = new BasicDigitalTwin()
            {
                Id = twinId,
                Metadata = new DigitalTwinMetadata
                {
                    ModelId = modelId
                }
            };


            var value = context.Message.SelectToken("Value.Value", false) ?? context.Message.SelectToken("Value", false);
            var timestamp = context.Message.SelectToken("Value.SourceTimestamp") ?? context.Message.SelectToken("SourceTimestamp", false);
            var applicationUri = context.Message.SelectToken("ApplicationUri", false);
            var nodeId = context.Message.SelectToken("NodeId", false);
            var displayName = context.Message.SelectToken("DisplayName");

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

            _logger.LogInformation($"Updating or Creating Twin {twinId} in {context.IngestionContext.AdtUrl}");

            await context.IngestionContext.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, twin);
        }
    }
}