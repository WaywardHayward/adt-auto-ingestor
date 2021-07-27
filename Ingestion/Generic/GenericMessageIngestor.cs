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

            if (!string.IsNullOrEmpty(twinId))
                _currentTwinId = deviceId;
            else
                _currentTwinId = eventData.SystemProperties.ContainsKey("iothub-connection-device-id") ? eventData.SystemProperties["iothub-connection-device-id"].ToString() : string.Empty;

        }

        private string GetTwinId(JObject message, string identifierPath)
        {
            var deviceId = message.SelectToken(identifierPath, false);
            return deviceId?.Value<string>();
        }

        private string GetTwinId(JObject message, string [] identifierPaths)
        {
            foreach(var path in identifierPaths){
                var deviceId = GetTwinId(message, path);
                if(!string.IsNullOrWhiteSpace(deviceId)){
                    _context.Log.LogInformation($"Found Twin Id in Message via Property Path {path}");
                     return deviceId;
                }
            }
            return null;
        }

        protected override async Task<string> EnsureModelExists(JObject message)
        {
            var rawModelId = $"dtmi:com:microsoft:autoingest:{_context.Configuration[Constants.INGESTION_EVENT_HUB_NAME].Replace("-", string.Empty).ToLower()}:{_currentTwinId.Replace("-", string.Empty).ToLower()}";
            return await EnsureModelExists(message, rawModelId, _currentTwinId.Split(":").LastOrDefault());
        }
    }
}