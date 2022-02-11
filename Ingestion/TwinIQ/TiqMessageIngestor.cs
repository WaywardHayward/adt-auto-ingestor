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

        public TwinIqMessageIngestor(ILogger<TwinIqMessageIngestor> log, DigitalTwinModelCache modelCache, TiqTwinIdProvider twinIdProvider) : base(modelCache, twinIdProvider, log)
        {        
        }

        public async Task Ingest(MessageContext context)
        {
            var twinId = _twinIdProvider.PopulateTwinId(context);
            
            if (string.IsNullOrWhiteSpace(twinId))
            {
                _logger.LogError($"No Node ID found on Twin IQ Message");
                return;
            }

            if(context.Message.SelectToken("Payload.SensorId", false)?.Value<string>() == "Heartbeat" || context.Message.SelectToken("Payload.SensorId", false)?.Value<string>() == "ModelManager" )
            {
                _logger.LogDebug($"Ignoring Heartbeat");
                return;
            }

            await WriteToTwin(context, twinId, await EnsureModelExists(context));
        }

        protected override async Task WriteToTwin(MessageContext context, string twinId, string modelId)
        {
            var twin = await GetTwin(context, twinId);

            if (twin == null)
                await CreateTwinIQSensorTwin(context, modelId, twinId);
            else
                await UpdateTwinIQSensorTwin(context, twin, twinId, modelId);
        }

        private async Task UpdateTwinIQSensorTwin(MessageContext context, BasicDigitalTwin twin, string twinId, string modelId)
        {
            var patch = new JsonPatchDocument();

            patch.AppendReplace("/$metadata/$model", modelId);

            var value = context.Message.SelectToken("Payload.Value", false);
            var timestamp = context.Message.SelectToken("Payload.TS", false);
            var uom = context.Message.SelectToken("Payload.UOM", false);
            var machineId = context.Message.SelectToken("Payload.MachineId", false);
            var nodeId = twinId;
            var tag = context.Message.SelectToken("Payload.Tag", false);

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


            await context.IngestionContext.DigitalTwinsClient.UpdateDigitalTwinAsync(twinId, patch, twin.ETag);
        }

        private async Task CreateTwinIQSensorTwin(MessageContext context, string modelId, string twinId)
        {
            var twin = new BasicDigitalTwin()
            {
                Id = twinId,
                Metadata = new DigitalTwinMetadata
                {
                    ModelId = modelId
                }
            };

            var value = context.Message.SelectToken("Payload.Value", false);
            var timestamp = context.Message.SelectToken("Payload.TS", false);
            var uom = context.Message.SelectToken("Payload.UOM", false);
            var nodeId = twinId;
            var tag = context.Message.SelectToken("Payload.Tag", false);
            var machineId = context.Message.SelectToken("Payload.MachineId", false);

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


            _logger.LogInformation($"Updating or Creating Twin {twinId} in {context.IngestionContext.AdtUrl}");

            await context.IngestionContext.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, twin);
        }

        protected override Task<string> EnsureModelExists(MessageContext context)
        {
            return EnsureModelExists(context, $"dtmi:com:microsoft:autoingest:twiniq:sensor", "Twin IQ Sensor Node");
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
            _logger.LogInformation($"Creating Auto Model {modelId} in {context.IngestionContext.AdtUrl}\n: {modelDtdl}");

            var newModels = await context.IngestionContext.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });

            if (newModels.Value.Any())
                _logger.LogInformation($"Created Auto Model {modelId} in {context.IngestionContext.AdtUrl}");

            return modelId;
        }
    }
}