using System;
using System.Linq;
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
            _twinIdentifiers =_configuration[Constants.INGESTION_ADT_TWIN_IDENTIFIERS]?.Split(";")?.Where(x => !string.IsNullOrWhiteSpace(x)).ToArray();      
            
        }

        public string PopulateTwinId(MessageContext context)
        {
            var deviceId = GetTwinId(context.Message, _twinIdentifiers ?? new[] { "message.DeviceId" });
            var currentTwinId = string.Empty;

            if (!string.IsNullOrEmpty(deviceId))
                currentTwinId = deviceId;

            if (string.IsNullOrEmpty(currentTwinId) && context.IngestionContext.EventData?.SystemProperties != null)
                currentTwinId = context.IngestionContext.EventData.SystemProperties.ContainsKey(Constants.IOT_DEVICE_ID_PROPERTY) ? context.IngestionContext.EventData.SystemProperties[Constants.IOT_DEVICE_ID_PROPERTY].ToString() : string.Empty;

            return currentTwinId;
        }

        private string GetTwinId(JObject message, string[] identifierPaths)
        {
            for(var index = 0; index < identifierPaths.Length; index++)
            {
                var identifierPath = identifierPaths[index];
                var identifier = GetTwinId(message, identifierPath);
                if (identifier != null)
                    return identifier.ToString();
            }
           
            return null;
        }

        private string GetTwinId(JObject message, string identifierPath)
        {
            _logger.LogInformation($"Looking For Twin Id {identifierPath.ToString()} in Event {message.ToString()}");            
            var deviceId = message.SelectToken(identifierPath.ToString(), false);
            return deviceId?.Value<string>();
        }


    }
}