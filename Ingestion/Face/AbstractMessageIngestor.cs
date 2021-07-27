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

namespace adt_auto_ingester.Ingestion.Face
{
    public abstract class AbstractMessageIngestor
    {
        protected readonly IngestionContext _context;
        protected AbstractMessageIngestor(IngestionContext context)
        {
            _context = context;
        }

        protected async Task<List<DigitalTwinModel>> GetAllModels()
        {
            var models = new List<DigitalTwinModel>();
            var modelsQuery = _context.DigitalTwinsClient.GetModelsAsync(new GetModelsOptions { IncludeModelDefinition = true });
            await foreach (var model in modelsQuery)
            {
                models.Add(JsonConvert.DeserializeObject<DigitalTwinModel>(model.DtdlModel));
            }
            return models;
        }

        protected abstract Task<string> EnsureModelExists(JObject message);

        protected virtual async Task<string> EnsureModelExists(JObject message, string rawModelId, string name)
        {
            var modelId = $"{rawModelId};1";
            var models = await GetAllModels();
            var modelExists = models.Any(m => m.Id.Split(";")[0] == rawModelId);
            var model = modelExists ? models.Where(m => m.Id.Split(";")[0] == rawModelId).OrderByDescending(m => int.Parse(m.Id.Split(";")[1])).FirstOrDefault() : null;

            if (model == null)
            {
                _context.Log.LogInformation($"Did Not Find Auto Model {modelId} in {_context.AdtUrl}");
                await CreateAutoIngestModel(message, name, modelId, model);
            }
            else
            {
                modelId = model.Id;
                _context.Log.LogInformation($"Model found for {modelId} {JsonConvert.SerializeObject(model)}");

                var messagePayLoad = message.SelectToken("message");

                if (messagePayLoad != null)
                    message = messagePayLoad as JObject;

                _context.Log.LogInformation("Checking Properties Exist in Model...");

                var modelContent = JsonConvert.DeserializeObject<DigitalTwinModel>(JsonConvert.SerializeObject(model));
                var modelVersion = int.Parse(model.Id.Split(";").LastOrDefault());
                var missingProperties = message.Properties().Where(p => modelContent.Contents.All(c => c.Name != p.Name));

                if (missingProperties.Any())
                {


                    _context.Log.LogInformation($"Found {missingProperties.Count()} missing Properties");

                    modelVersion++;

                    foreach (var missingProperty in missingProperties)
                        modelContent.Contents.Add(new DigitalTwinModelPropertyContent
                        {
                            Name = missingProperty.Name,
                            Schema = "string",
                            Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
                        });


                    modelId = $"{rawModelId};{modelVersion}";
                    modelContent.Id = modelId;
                    var modelDtdl = JsonConvert.SerializeObject(modelContent, Formatting.Indented);

                    _context.Log.LogInformation($"Creating Auto Model {modelId} in {_context.AdtUrl}\n: {modelDtdl}");
                    await _context.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });
                }
                else
                {
                    _context.Log.LogInformation("No Properties missing");
                }


            }

            return modelId;
        }

        private async Task CreateAutoIngestModel(JObject message, string name, string modelId, DigitalTwinModel model)
        {

            var newModel = new DigitalTwinModel
            {
                Id = modelId,
                DisplayName = $"{name.Substring(0,Math.Min(30, name.Length))} Auto Provisioned Model"
            };

            var messagePayLoad = message.SelectToken("message");

            if (messagePayLoad != null)
                message = messagePayLoad as JObject;

            foreach (var property in message.Properties())
            {
                newModel.Contents.Add(new DigitalTwinModelPropertyContent
                {
                    Name = property.Name,
                    Schema = "string",
                    Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
                });
            }

            var modelDtdl = JsonConvert.SerializeObject(newModel, Formatting.Indented);
            _context.Log.LogInformation($"Creating Auto Model {modelId} in {_context.AdtUrl}\n: {modelDtdl}");
            var models = await _context.DigitalTwinsClient.CreateModelsAsync(new string[] { modelDtdl });

            if (models.Value.Any())
                _context.Log.LogInformation($"Created Auto Model {modelId} in {_context.AdtUrl}");

        }

        protected virtual async Task WriteToTwin(JObject message, string twinId, string modelId)
        {
            var twin = await GetTwin(twinId);

            var messagePayLoad = message.SelectToken("message");

            if (messagePayLoad != null)
            {
                _context.Log.LogInformation($"Found message payload {message["message"].ToString()}");
                message = messagePayLoad.Value<JObject>();
            }


            if (twin == null)
            {
                await CreateNewTwin(message, twinId, modelId);
            }
            else
            {
                await UpdateExistingTwin(message, twinId, modelId, twin);
            }

            _context.Log.LogInformation($"Twin Data Applied to  {twinId} in {_context.AdtUrl}");
        }

        protected async Task UpdateExistingTwin(JObject message, string twinId, string modelId, BasicDigitalTwin twin)
        {
            var patch = new JsonPatchDocument();

            patch.AppendReplace("/$metadata/$model", modelId);

            foreach (var property in message?.Properties())
            {
                if (twin.Contents.Any(c => c.Key == property.Name))
                    patch.AppendReplace("/" + property.Name, message.SelectToken(property.Name).ToString());
                else
                    patch.AppendAdd("/" + property.Name, message.SelectToken(property.Name).ToString());
            }

            await _context.DigitalTwinsClient.UpdateDigitalTwinAsync(twinId, patch, twin.ETag);
        }

        protected async Task CreateNewTwin(JObject message, string twinId, string modelId)
        {
            var newTwin = new BasicDigitalTwin()
            {
                Id = twinId,
                Metadata = new DigitalTwinMetadata
                {
                    ModelId = modelId
                }
            };

            foreach (var property in message?.Properties())
                newTwin.Contents.Add(property.Name, message.SelectToken(property.Name).ToString());

            _context.Log.LogInformation($"Updating or Creating Twin {twinId} in {_context.AdtUrl}");

            await _context.DigitalTwinsClient.CreateOrReplaceDigitalTwinAsync(twinId, newTwin);
        }

        protected async Task<BasicDigitalTwin> GetTwin(string twinId)
        {
            try
            {
                return await _context.DigitalTwinsClient.GetDigitalTwinAsync<BasicDigitalTwin>(twinId);
            }
            catch (Exception ex)
            {
                _context.Log.LogDebug($"Twin Probably Not Found:\n{ex}");
                _context.Log.LogInformation($"Twin with Id {twinId} does not exist");
            }
            return null;
        }

        protected void AddPropertyPatch(JsonPatchDocument patch, BasicDigitalTwin twin, string propertyName, string propertyValue)
        {
            if (twin.Contents.Any(c => c.Key == propertyName))
                patch.AppendReplace("/" + propertyName, propertyValue);
            else
                patch.AppendAdd("/" + propertyName, propertyValue);
        }
    }
}