// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingestor.AzureDigitalTwins;
using Azure;
using Azure.DigitalTwins.Core;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.TwinIQ
{
    public class TwinIqMessageIngestor: AbstractMessageIngestor, IMessageIngestor
    {
        private string _currentTwinId;

        public TwinIqMessageIngestor(IngestionContext context, DigitalTwinModelCache modelCache) : base(context, modelCache)
        {
            context.Log.LogInformation($"Processing Twin IQ Message");
        }

        public async Task Ingest(EventData eventData, JObject message)
        {
            PopulateTwinId(message);
            if (string.IsNullOrWhiteSpace(_currentTwinId))
            {
                _context.Log.LogError($"No Node ID found on Twin IQ Message");
                return;
            }

            if(message.SelectToken("Payload.SensorId", false)?.Value<string>() == "Heartbeat" || message.SelectToken("Payload.SensorId", false)?.Value<string>() == "ModelManager" )
            {
                _context.Log.LogDebug($"Ignoring Heartbeat");
                return;
            }

            await WriteToTwin(message, _currentTwinId, await EnsureModelExists(message));
        }

        private void PopulateTwinId(JObject message)
        {
            var nodeId = message.SelectToken("Routing.TiqTwin.NodeId", true);
            if (nodeId != null)
                _currentTwinId = nodeId.Value<string>();
        }

        protected override async Task WriteToTwin(JObject message, string twinId, string modelId)
        {
            var twin = await GetTwin(twinId);

            if (twin == null)
                await CreateTwinIQSensorTwin(message, modelId, twinId);
            else
                await UpdateTwinIQSensorTwin(message, twin, twinId, modelId);
        }

        private async Task UpdateTwinIQSensorTwin(JObject message, BasicDigitalTwin twin, string twinId, string modelId)
        {
            var patch = new JsonPatchDocument();

            patch.AppendReplace("/$metadata/$model", modelId);

            var value = message.SelectToken("Payload.Value", false);
            var timestamp = message.SelectToken("Payload.TS", false);
            var uom = message.SelectToken("Payload.UOM", false);
            var machineId = message.SelectToken("Payload.MachineId", false);
            var nodeId = twinId;
            var tag = message.SelectToken("Payload.Tag", false);

            if(tag != null && machineId != null)
            {
                twin.Contents.Add("DisplayName", $"{machineId.ToString()}-{tag.ToString()}");
            }

            if (tag!= null)
            {                
                twin.Contents.Add("HistoricalTagName", tag.ToString());
            }

            if (value!= null)
                twin.Contents.Add("Value", value.ToString());

            if (timestamp!= null)
                twin.Contents.Add("Timestamp", timestamp.ToString());

            if (uom!= null)
                twin.Contents.Add("UoM", uom.ToString());


            await _context.DigitalTwinsClient.UpdateDigitalTwinAsync(twinId, patch, twin.ETag);
        }

        private async Task CreateTwinIQSensorTwin(JObject message, string modelId, string twinId)
        {
            var twin = new BasicDigitalTwin()
            {
                Id = twinId,
                Metadata = new DigitalTwinMetadata
                {
                    ModelId = modelId
                }
            };

            var value = message.SelectToken("Payload.Value", false);
            var timestamp = message.SelectToken("Payload.TS", false);
            var uom = message.SelectToken("Payload.UOM", false);
            var nodeId = twinId;
            var tag = message.SelectToken("Payload.Tag", false);
            var machineId = message.SelectToken("Payload.MachineId", false);

            if (value!= null)
                twin.Contents.Add("Value", value.ToString());

            if(tag!= null && machineId.HasValues == true)
            {
                twin.Contents.Add("DisplayName", $"{machineId.ToString()}-{tag.ToString()}");
            }

            if (tag!= null)
            {                
                twin.Contents.Add("HistoricalTagName", tag.ToString());
            }

            if (timestamp!= null)
                twin.Contents.Add("Timestamp", timestamp.ToString());

            if (uom!= null)
                twin.Contents.Add("UoM", uom.ToString());


            _context.Log.LogInformation($"Updating or Creating Twin {twinId} in {_context.AdtUrl}");

            await _context.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, twin);
        }

        protected override Task<string> EnsureModelExists(JObject message)
        {
            return EnsureModelExists(message, $"dtmi:com:microsoft:autoingest:twiniq:sensor", "Twin IQ Sensor Node");
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
                DisplayName = $"Twin IQ Sensor Node Auto Provisioned Model"
            };

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "DisplayName",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "HistoricalTagName",
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
                Name = "TagValidationMin",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "TagValidationMax",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

             opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "Timestamp",
                Schema = "string",
                Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
            });

            opcNodeModel.Contents.Add(new DigitalTwinModelPropertyContent
            {
                Name = "UoM",
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
    }
}