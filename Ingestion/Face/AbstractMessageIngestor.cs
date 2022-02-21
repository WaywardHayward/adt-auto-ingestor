// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using adt_auto_ingester.AzureDigitalTwins;
using Azure.DigitalTwins.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure;
using adt_auto_ingestor.AzureDigitalTwins;
using System.Globalization;

namespace adt_auto_ingester.Ingestion.Face
{
    public abstract class AbstractMessageIngestor
    {
        protected readonly DigitalTwinModelCache _modelCache;
        protected readonly ITwinIdProvider _twinIdProvider;
        protected readonly ILogger _logger;

        protected AbstractMessageIngestor(DigitalTwinModelCache modelCache, ITwinIdProvider twinIdProvider, ILogger log)
        {
            _modelCache = modelCache;
            _twinIdProvider = twinIdProvider;
            _logger = log;
        }

        protected Task<List<DigitalTwinModel>> GetAllModels(string modelId) => _modelCache.GetModelsForId(modelId);
        protected abstract Task<string> EnsureModelExists(MessageContext context);

        protected virtual async Task<string> EnsureModelExists(MessageContext context, string rawModelId, string name)
        {
            var modelId = $"{rawModelId};1";
            var models = await GetAllModels(rawModelId);
            var model =  models.OrderByDescending(m => m.ModelVersion.Value).FirstOrDefault();

            if (model == null)
            {
                _logger.LogWarning($"Did Not Find Auto Model {modelId} in {context.IngestionContext.AdtUrl}");
                await CreateAutoIngestModel(context, name, modelId, model);
            }
            else
            {
                modelId = model.Id;
                _logger.LogTrace($"Model found for {modelId}");
                _logger.LogTrace("Checking Properties Exist in Model...");

                var modelContent = JsonConvert.DeserializeObject<DigitalTwinModel>(JsonConvert.SerializeObject(model));
                var modelVersion = model.ModelVersion.Value;
                var missingProperties = context.MessageProperties.Value.Keys.Where(p => !modelContent.PropertyContents.Value.ContainsKey(p));

                if (missingProperties.Any())
                {

                    _logger.LogTrace($"Found {missingProperties.Count()} missing Properties");

                    modelVersion++;

                    foreach (var missingProperty in missingProperties)
                        modelContent.Contents.Add(new DigitalTwinModelPropertyContent
                        {
                            Name = missingProperty,
                            Schema = "string",
                            Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
                        });


                    modelId = $"{rawModelId};{modelVersion}";
                    modelContent.Id = modelId;
                    var modelDtdl = JsonConvert.SerializeObject(modelContent, Formatting.Indented);

                    _logger.LogTrace($"Creating Auto Model {modelId} in {context.IngestionContext.AdtUrl}\n: {modelDtdl}");
                    await context.IngestionContext.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });
                    _modelCache.AddModel(modelId, modelContent);
                }
                else
                {
                    _logger.LogTrace("No Properties missing");
                }


            }

            return modelId;
        }

        private async Task CreateAutoIngestModel(MessageContext context, string name, string modelId, DigitalTwinModel model)
        {

            var newModel = new DigitalTwinModel
            {
                Id = modelId,
                DisplayName = $"{name.Substring(0, Math.Min(30, name.Length))} Auto Provisioned Model"
            };
          
            foreach (var property in context.MessageProperties.Value.Keys)
            {
                newModel.Contents.Add(new DigitalTwinModelPropertyContent
                {
                    Name = property,
                    Schema = "string",
                    Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
                });
            }

            var modelDtdl = JsonConvert.SerializeObject(newModel, Formatting.Indented);
            _logger.LogTrace($"Creating Auto Model {modelId} in {context.IngestionContext.AdtUrl}\n: {modelDtdl}");
            var models = await context.IngestionContext.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });

            if (models.Value.Any())
                _logger.LogTrace($"Created Auto Model {modelId} in {context.IngestionContext.AdtUrl}");

        }

        protected virtual async Task WriteToTwin(MessageContext context, string twinId, string modelId)
        {
            var twin = await GetTwin(context, twinId);
           
            if (twin == null)
            {
                await CreateNewTwin(context, twinId, modelId);
            }
            else
            {
                await UpdateExistingTwin(context, twinId, modelId, twin);
            }

            _logger.LogTrace($"Twin Data Applied to  {twinId} in {context.IngestionContext.AdtUrl}");
        }

        protected async Task UpdateExistingTwin(MessageContext context, string twinId, string modelId, BasicDigitalTwin twin)
        {
            var patch = new JsonPatchDocument();

            patch.AppendReplace("/$metadata/$model", modelId);

            var sourceTimestamp = GetSourceTimestamp(context); 

            foreach (var property in context.MessageProperties.Value.Keys)
            {
                if (twin.Contents.ContainsKey(property))
                    patch.AppendReplace("/" + property, ((JValue)context.Message?.SelectToken(property)).ToString(CultureInfo.InvariantCulture));
                else
                    patch.AppendAdd("/" + property, ((JValue)context.Message?.SelectToken(property))?.ToString(CultureInfo.InvariantCulture));

                patch.AppendReplace($"/$metadata/{property}/sourceTime", sourceTimestamp);
            }

            await context.IngestionContext.DigitalTwinsClient.UpdateDigitalTwinAsync(twinId, patch, twin.ETag);
        }

        protected abstract string GetSourceTimestamp(MessageContext context);

        protected async Task CreateNewTwin(MessageContext context, string twinId, string modelId)
        {
            var newTwin = new BasicDigitalTwin()
            {
                Id = twinId,
                Metadata = new DigitalTwinMetadata
                {
                    ModelId = modelId
                }
            };

            var sourceTimestamp = GetSourceTimestamp(context); 

            foreach (var property in context.MessageProperties.Value.Keys){
                newTwin.Contents.Add(property, ((JValue)context.Message.SelectToken(property)).ToString(CultureInfo.InvariantCulture));           
            }

            _logger.LogTrace($"Updating or Creating Twin {twinId} in {context.IngestionContext.AdtUrl}");

            await context.IngestionContext.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, newTwin);
        }

        protected async Task<BasicDigitalTwin> GetTwin(MessageContext context, string twinId)
        {
            try
            {
                return await context.IngestionContext.DigitalTwinsClient.GetDigitalTwinAsync<BasicDigitalTwin>(twinId);
            }
            catch (Exception ex)
            {
                _logger.LogDebug($"Twin Probably Not Found:\n{ex}");
                _logger.LogTrace($"Twin with Id {twinId} does not exist");
            }
            return null;
        }

        protected void AddPropertyPatch(JsonPatchDocument patch, string propertyName, string propertyValue, MessageContext context)
        {
            patch.AppendAdd("/" + propertyName, propertyValue);
            patch.AppendAdd("/$metadata/" + propertyName + "/sourceTime",GetSourceTimestamp(context));
        }
    }
}