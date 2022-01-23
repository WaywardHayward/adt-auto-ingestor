using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.Ingestion;
using Azure.DigitalTwins.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace adt_auto_ingestor.AzureDigitalTwins
{
    public class DigitalTwinModelCache
    {
        private Dictionary<string, DigitalTwinModel> _modelCache = new Dictionary<string, DigitalTwinModel>();

        private DateTime _nextModelFetch = DateTime.MinValue;
        public IngestionContext Context { get; internal set; }


        public DigitalTwinModelCache()
        {
        }

        public async Task<List<DigitalTwinModel>> GetModels()
        {
           if (_nextModelFetch > DateTime.UtcNow)
            {
                Context.Log.LogTrace("Using model Cache");
                return _modelCache.Values.ToList();
            }
            Context.Log.LogInformation("All Keys {0}", string.Join(",", _modelCache.Keys));
            Context.Log.LogInformation("Fetching all models From Twin");

            var models = await GetModelsFromTwin();

            lock (_modelCache)
            {
                _modelCache.Clear();

                foreach (var model in models)
                {
                    if (!_modelCache.ContainsKey(model.Id))
                        _modelCache.Add(model.Id, model);
                }

                _nextModelFetch = DateTime.UtcNow.AddMinutes(5);

            }

            return models;
        }

        private async Task<List<DigitalTwinModel>> GetModelsFromTwin()
        {
            var models = new List<DigitalTwinModel>();
            var modelsQuery = Context.DigitalTwinsClient.GetModelsAsync(new GetModelsOptions { IncludeModelDefinition = true });
            await foreach (var model in modelsQuery)
            {
                try
                {
                    models.Add(JsonConvert.DeserializeObject<DigitalTwinModel>(model.DtdlModel));
                }
                catch (Exception ex)
                {
                    Context.Log.LogError(ex, $"Error Deserializing Model {model.Id}");
                }
            }

            return models;
        }

        internal void AddModel(string modelId, DigitalTwinModel modelContent)
        {
            if(!_modelCache.ContainsKey(modelId))
                _modelCache.Add(modelId, modelContent);
        }
    }
}