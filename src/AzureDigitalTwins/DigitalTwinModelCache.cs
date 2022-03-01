using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.AzureDigitalTwins.Face;
using adt_auto_ingester.Helpers;
using Azure.DigitalTwins.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace adt_auto_ingestor.AzureDigitalTwins
{
    public class DigitalTwinModelCache
    {
        private Dictionary<string, List<DigitalTwinModel>> _modelCache = new Dictionary<string, List<DigitalTwinModel>>();

        private DateTime _nextModelFetch = DateTime.MinValue;
        private readonly IDigitalTwinsClientProvider _digitalTwinClientProvider;
        private readonly LoggingAdapter _logger;

        public DigitalTwinModelCache(LoggingAdapter logger, IDigitalTwinsClientProvider clientProvider)
        {
            _digitalTwinClientProvider = clientProvider;
            _logger = logger;
        }

        public bool ModelIsCached(string modelId)
        {
            return _modelCache.ContainsKey(modelId);
        }

        public async Task<List<DigitalTwinModel>> GetModelsForId(string modelId){
            if (_nextModelFetch > DateTime.UtcNow)
            {
                _logger.LogDebug("Using model Cache");
                return _modelCache.ContainsKey(modelId) ? _modelCache[modelId] : new List<DigitalTwinModel>();
            }
            await RebuildCache();
            return await GetModelsForId(modelId);         
        }

        public async Task<List<DigitalTwinModel>> GetModels()
        {
            if (_nextModelFetch > DateTime.UtcNow)
            {
                _logger.LogInformation("Using model Cache");
                return _modelCache.Values.SelectMany(s => s).ToList();
            }
            _logger.LogInformation("All Keys {0}", string.Join(",", _modelCache.Keys));
            _logger.LogInformation("Fetching all models From Twin");
            return await RebuildCache();
        }

        private async Task<List<DigitalTwinModel>> RebuildCache()
        {
            var models = await GetModelsFromTwin();

            lock (_modelCache)
            {
                _modelCache.Clear();

                for (int i = 0; i < models.Count; i++)
                {
                    DigitalTwinModel model = models[i];
                    if (!_modelCache.ContainsKey(model.ModelId.Value))
                        _modelCache.Add(model.ModelId.Value, new List<DigitalTwinModel>() { model });
                    else
                        _modelCache[model.ModelId.Value].Add(model);
                }

                _nextModelFetch = DateTime.UtcNow.AddMinutes(5);

            }

            return models;
        }

        private async Task<List<DigitalTwinModel>> GetModelsFromTwin()
        {
            var models = new List<DigitalTwinModel>();
            var modelsQuery = _digitalTwinClientProvider.GetClient().GetModelsAsync(new GetModelsOptions { IncludeModelDefinition = true });
            await foreach (var model in modelsQuery)
            {
                try
                {
                    models.Add(JsonConvert.DeserializeObject<DigitalTwinModel>(model.DtdlModel));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error Deserializing Model {model.Id}");
                }
            }

            return models;
        }

        internal void AddModel(string modelId, DigitalTwinModel modelContent)
        {
            if(!_modelCache.ContainsKey(modelContent.ModelId.Value))
                _modelCache.Add(modelId, new List<DigitalTwinModel> { modelContent});
            else
                _modelCache[modelContent.ModelId.Value].Add(modelContent);
            
        }
    }
}