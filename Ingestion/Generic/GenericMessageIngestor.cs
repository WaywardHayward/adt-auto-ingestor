// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingester.Models;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.Generic
{
    public class GenericMessageIngestor : AbstractMessageIngestor, IMessageIngestor
    {

        private string _currentTwinId;

        public GenericMessageIngestor(IngestionContext context) : base(context)
        {
            context.Log.LogInformation($"Processing Generic Message");
        }

        public async Task Ingest(EventData eventData, JObject message)
        {
            PopulateTwinId(eventData, message);

            _context.Log.LogInformation("Checking For Twin Id in Event");

            if (string.IsNullOrWhiteSpace(_currentTwinId))
            {
                _context.Log.LogWarning($"Message {message.ToString()} has no deviceId ");
                return;
            }

            if (message.SelectToken("Payload.SensorId", false)?.Value<string>() == "Heartbeat" || message.SelectToken("Payload.SensorId", false)?.Value<string>() == "ModelManager")
            {
                _context.Log.LogDebug($"Ignoring Heartbeat");
                return;
            }

            await WriteToTwin(message, _currentTwinId, await EnsureModelExists(message));
        }
        private void PopulateTwinId(EventData eventData, JObject message)
        {
            var twinId = string.Empty;
            var deviceId = GetTwinId(message, _context.Configuration[Constants.INGESTION_ADT_TWIN_IDENTIFIERS]?.Split(";") ?? new[] { "message.DeviceId" });

            if (!string.IsNullOrEmpty(deviceId))
                _currentTwinId = deviceId; 
            else
                _currentTwinId = eventData.SystemProperties.ContainsKey("iothub-connection-device-id") ? eventData.SystemProperties["iothub-connection-device-id"].ToString() : string.Empty;

        }

        private string GetTwinId(JObject message, string identifierPath)
        {
            _context.Log.LogInformation($"Looking For Twin Id {identifierPath} in Event");
            var deviceId = message.SelectToken(identifierPath, false);
            return deviceId?.Value<string>();
        }

        private string GetTwinId(JObject message, string [] identifierPaths)
        {
            foreach(var path in identifierPaths){
                var deviceId = GetTwinId(message, path);
                if(!string.IsNullOrWhiteSpace(deviceId)){
                    _context.Log.LogInformation($"Found Twin Id {deviceId} in Message via Property Path {path}");
                     return deviceId;
                }
            }
            return null;
        }

        protected override async Task<string> EnsureModelExists(JObject message)
        {
            var modelId = GetModelId(message, _context.Configuration[Constants.INGESTION_ADT_MODEL_IDENTIFIERS]?.Split(";")) ?? $"{_context.Configuration[Constants.INGESTION_EVENT_HUB_NAME].Replace("-", string.Empty).ToLower()}:{_currentTwinId.Replace("-", string.Empty).ToLower()}";
            var rawModelId = $"dtmi:com:microsoft:autoingest:{modelId}";
            return await EnsureModelExists(message, rawModelId, modelId ?? _currentTwinId.Split(":").LastOrDefault());
        }

        private string GetModelId(JObject message, string[] identifierPaths)
        {
            if(identifierPaths == null)
                return null;

            foreach (var path in identifierPaths)
            {
                var modelId = message.SelectToken(path, false)?.Value<string>();

                if (!string.IsNullOrWhiteSpace(modelId))
                {
                    _context.Log.LogInformation($"Found Model Id {modelId} in Message via Property Path {path}");
                    return modelId;
                }
            }
            return null;
        }
    }
}