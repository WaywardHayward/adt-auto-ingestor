using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingester.Models;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.Generic
{
    public class GenericMessageTwinIdProvider : ITwinIdProvider
    {
        private readonly ILogger<GenericMessageTwinIdProvider> _logger;
        private readonly IConfiguration _configuration;
        private readonly string[] _twinIdentifiers;

        public GenericMessageTwinIdProvider(IConfiguration configuration, ILogger<GenericMessageTwinIdProvider> log)
        {
            _logger = log;
            _configuration = configuration;                 
            _twinIdentifiers =_configuration[Constants.INGESTION_ADT_TWIN_IDENTIFIERS]?.Split(";");      
        }

        public string PopulateTwinId(MessageContext context)
        {
            var deviceId = GetTwinId(context.Message, _twinIdentifiers ?? new[] { "message.DeviceId" });
            var currentTwinId = string.Empty;

            if (!string.IsNullOrEmpty(deviceId))
                currentTwinId = deviceId;

            if (string.IsNullOrEmpty(currentTwinId))
                currentTwinId = context.IngestionContext.EventData.SystemProperties.ContainsKey("iothub-connection-device-id") ? context.IngestionContext.EventData.SystemProperties["iothub-connection-device-id"].ToString() : string.Empty;

            return currentTwinId;
        }

        private string GetTwinId(JObject message, string[] identifierPaths)
        {
            foreach (var path in identifierPaths)
            {
                var deviceId = GetTwinId(message, path);
                if (!string.IsNullOrWhiteSpace(deviceId))
                {
                    _logger.LogDebug($"Found Twin Id {deviceId} in Message via Property Path {path}");
                    return deviceId;
                }
            }
            return null;
        }

        private string GetTwinId(JObject message, string identifierPath)
        {
            _logger.LogInformation($"Looking For Twin Id {identifierPath} in Event {message.ToString()}");
            var deviceId = message.SelectToken(identifierPath, false);
            return deviceId?.Value<string>();
        }


    }
}