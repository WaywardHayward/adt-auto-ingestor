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
using Microsoft.Extensions.Configuration;
using adt_auto_ingester.Models;
using src.AzureDigitalTwins.Builder;

namespace adt_auto_ingester.Ingestion.Face
{
    public abstract class AbstractMessageIngestor
    {
        protected readonly DigitalTwinModelCache _modelCache;
        protected readonly ITwinIdProvider _twinIdProvider;
        protected readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        protected readonly string[] _timestampIdentifiers;
        protected readonly string[] _modelIdentifiers;
        protected readonly string[] _twinIdentifiers;
        private readonly DigitalTwinCache _twinCache;
        private readonly TwinPatchBuilder _patchBuilder;

        protected AbstractMessageIngestor(DigitalTwinModelCache modelCache, ITwinIdProvider twinIdProvider, ILogger log, IConfiguration configuration, DigitalTwinCache twinCache)
        {
            _modelCache = modelCache;
            _twinIdProvider = twinIdProvider;
            _logger = log;
            _configuration = configuration;
            _timestampIdentifiers = _configuration[Constants.INGESTION_ADT_TIMESTAMP_IDENTIFIERS]?.Split(";");
            _modelIdentifiers = _configuration[Constants.INGESTION_ADT_MODEL_IDENTIFIERS]?.Split(";");
            _twinIdentifiers =_configuration[Constants.INGESTION_ADT_TWIN_IDENTIFIERS]?.Split(";");            
            _twinCache = twinCache;
            _patchBuilder = new TwinPatchBuilder();
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
                _logger.LogDebug($"Model found for {modelId}");
                _logger.LogDebug("Checking Properties Exist in Model...");

                var modelContent = JsonConvert.DeserializeObject<DigitalTwinModel>(JsonConvert.SerializeObject(model));
                var modelVersion = model.ModelVersion.Value;
                var missingProperties = context.MessageProperties.Value.Keys.Where(p => !modelContent.PropertyContents.Value.ContainsKey(p));

                if (missingProperties.Any())
                {

                    _logger.LogDebug($"Found {missingProperties.Count()} missing Properties");

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

                    _logger.LogDebug($"Creating Auto Model {modelId} in {context.IngestionContext.AdtUrl}\n: {modelDtdl}");
                    await context.IngestionContext.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });
                    _modelCache.AddModel(modelId, modelContent);
                }
                else
                {
                    modelId = model.Id;
                    _logger.LogDebug("No Properties missing");
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
            _logger.LogDebug($"Creating Auto Model {modelId} in {context.IngestionContext.AdtUrl}\n: {modelDtdl}");
            var models = await context.IngestionContext.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });

            if (models.Value.Any())
                _logger.LogDebug($"Created Auto Model {modelId} in {context.IngestionContext.AdtUrl}");

        }

        protected virtual async Task WriteToTwin(MessageContext context, string twinId, string modelId)
        {
            var twinExists =  await _twinCache.TwinExists(twinId).ConfigureAwait(true);

            if (!twinExists)
            {
                _logger.LogDebug($"Twin {twinId} does not exist. Creating...");
                await CreateNewTwin(context, twinId, modelId);
            }
            else
            {
                _logger.LogDebug($"Twin {twinId} exists. Updating...");
                await UpdateExistingTwin(context, twinId, modelId, await _twinCache.GetTwin(twinId));
            }

            _logger.LogDebug($"Twin Data Applied to  {twinId} in {context.IngestionContext.AdtUrl}");
        }

        protected async Task UpdateExistingTwin(MessageContext context, string twinId, string modelId, BasicDigitalTwin twin)
        {
            var patch = _patchBuilder.Build(context, modelId, twin, GetSourceTimestamp(context));
            var updatedTwin = await context.IngestionContext.DigitalTwinsClient.UpdateDigitalTwinAsync(twinId, patch);
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
                newTwin.Metadata.PropertyMetadata.Add(property, new DigitalTwinPropertyMetadata
                {
                    SourceTime = new DateTimeOffset(DateTime.Parse(sourceTimestamp), TimeSpan.Zero)
                });     
            }

            _logger.LogDebug($"Updating or Creating Twin {twinId} in {context.IngestionContext.AdtUrl}");
            var createdTwin = await context.IngestionContext.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, newTwin);
            _twinCache.CacheTwin(twinId,createdTwin);
        }

        protected async Task<BasicDigitalTwin> GetTwin(MessageContext context, string twinId)
        {
            return await _twinCache.GetTwin(twinId);
        }

        protected void AddPropertyPatch(JsonPatchDocument patch, string propertyName, string propertyValue, MessageContext context)
        {
            patch.AppendAdd("/" + propertyName, propertyValue);
            patch.AppendAdd("/$metadata/" + propertyName + "/sourceTime",GetSourceTimestamp(context));
        }
    }
}