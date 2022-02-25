using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.AzureDigitalTwins.Face;
using Azure.DigitalTwins.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace adt_auto_ingestor.AzureDigitalTwins
{

    public class DigitalTwinCache
    {

        private readonly ILogger<DigitalTwinCache> _logger;
        private readonly IConfiguration _configuration;
        private readonly IDigitalTwinsClientProvider _digitalTwinsClientProvider;
        private readonly DigitalTwinsClient _client;
        private readonly Timer _cacheTimer;
        private readonly ConcurrentDictionary<string, BasicDigitalTwin> _digitalTwins = new ConcurrentDictionary<string, BasicDigitalTwin>();

        public DigitalTwinCache(ILogger<DigitalTwinCache> logger, IConfiguration configuration, IDigitalTwinsClientProvider digitalTwinsClientProvider)
        {
            _logger = logger;
            _configuration = configuration;
            _digitalTwinsClientProvider = digitalTwinsClientProvider;
            _client = _digitalTwinsClientProvider.GetClient();
            _cacheTimer = new Timer(async (state) => await RebuildCache(), null, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(10));
        }

        public async Task<bool> TwinExists(string twinId)
        {
            if (_digitalTwins.ContainsKey(twinId) && _digitalTwins[twinId] != null)
            {
                return _digitalTwins[twinId] != null;
            }

            var twin = await LoadTwin(twinId).ConfigureAwait(true);

            return twin != null;
        }

        private async Task<BasicDigitalTwin> LoadTwin(string twinId)
        {
            try
            {
                var twin = await _client.GetDigitalTwinAsync<BasicDigitalTwin>(twinId).ConfigureAwait(true);
                return CacheTwin(twinId, twin.Value);
            }
            catch (Exception ex)
            {
                CacheTwin(twinId, null);
                _logger.LogDebug($"Twin Probably Not Found:\n{ex}");
                _logger.LogDebug($"Twin with Id {twinId} does not exist");
            }
            return null;
        }

        internal BasicDigitalTwin CacheTwin(string twinId, BasicDigitalTwin twin)
        {
            if (_digitalTwins.ContainsKey(twinId))
                _digitalTwins[twinId] = twin;
            else
                _digitalTwins.AddOrUpdate(twinId, (s) => twin, (s, t) => twin);

            return _digitalTwins[twinId];
        }

        public async Task<BasicDigitalTwin> GetTwin(string twinId, bool fromTwin = false)
        {
            if (fromTwin)
                return await LoadTwin(twinId).ConfigureAwait(true);

            if (_digitalTwins.ContainsKey(twinId))
                return _digitalTwins[twinId];

            return null;
        }

        private async Task RebuildCache()
        {
            _logger.LogInformation("Rebuilding DigitalTwin Cache");

            if (_digitalTwins.Count == 0)
            {
                _logger.LogInformation("No DigitalTwins to Rebuild");
                return;
            }

            var twinsToLoad = _digitalTwins.Keys.ToList();

            foreach (var twinId in twinsToLoad)
            {
                await LoadTwin(twinId).ConfigureAwait(true);
            }

            _logger.LogInformation($"Reloaded {twinsToLoad.Count} Digital Twins");

        }

    }


}