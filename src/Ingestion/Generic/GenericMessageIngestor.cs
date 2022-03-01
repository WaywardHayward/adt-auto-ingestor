// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.Helpers;
using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingester.Models;
using adt_auto_ingestor.AzureDigitalTwins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.Generic
{
    public class GenericMessageIngestor : AbstractMessageIngestor, IMessageIngestor
    {

        public GenericMessageIngestor(LoggingAdapter log, DigitalTwinModelCache modelCache, GenericMessageTwinIdProvider twinIdProvider, IConfiguration configuration, DigitalTwinCache twinCache) : base(modelCache, twinIdProvider, log, configuration, twinCache)
        {

        }

        public async Task Ingest(MessageContext context)
        {
            try
            {
                var twinId = _twinIdProvider.PopulateTwinId(context);

                _logger.LogDebug("Checking For Twin Id in Event");

                if (string.IsNullOrWhiteSpace(twinId))
                {
                    _logger.LogWarning($"Message {context.Message.ToString()} has no deviceId ");
                    return;
                }

                if (context.Message.SelectToken("Payload.SensorId", false)?.Value<string>() == "Heartbeat" || context.Message.SelectToken("Payload.SensorId", false)?.Value<string>() == "ModelManager")
                {
                    _logger.LogDebug($"Ignoring Heartbeat");
                    return;
                }

                await WriteToTwin(context, twinId, await EnsureModelExists(context));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error Ingesting Generic Message {context.Message.ToString()}");
            }
        }


        protected override async Task<string> EnsureModelExists(MessageContext context)
        {
            var twinId = _twinIdProvider.PopulateTwinId(context);
            var modelId = GetModelId(context.Message,  _modelIdentifiers) ?? $"{context.IngestionContext.Configuration[Constants.INGESTION_EVENT_HUB_NAME].Replace("-", string.Empty).ToLower()}:{twinId.Replace("-", string.Empty).ToLower()}";
            var rawModelId = $"dtmi:com:microsoft:autoingest:{modelId}";
            return await EnsureModelExists(context, rawModelId, modelId ?? twinId.Split(":").LastOrDefault());
        }

        private string GetModelId(JObject message, string[] identifierPaths)
        {
            if (identifierPaths == null)
                return null;

            for (int i = 0; i < identifierPaths.Length; i++)
            {
                string path = identifierPaths[i];
                var modelId = message.SelectToken(path, false)?.Value<string>();

                if (!string.IsNullOrWhiteSpace(modelId))
                {
                    _logger.LogDebug($"Found Model Id {modelId} in Message via Property Path {path}");
                    return modelId;
                }
            }
            return null;
        }

        protected override string GetSourceTimestamp(MessageContext context)        
        {
            var defaultValue = DateTime.UtcNow.ToString("o");
           
            if (_timestampIdentifiers == null || _timestampIdentifiers.Length == 0)
            {
                _logger.LogDebug($"\t No Timestamp Identifiers Found in Configuration");
                return defaultValue;
            }

            for (int i = 0; i < _timestampIdentifiers.Length; i++)
            {
                string timestampIdentifier = _timestampIdentifiers[i];
                var timestamp = context.Message.SelectDateTimeTokenString(timestampIdentifier);

                if (timestamp == null)
                {
                    _logger.LogDebug($"\t No Timestamp Found in Message via Property Path {timestampIdentifier}");
                    continue;
                }
                
                return timestamp;
            }
            
            return defaultValue;
        }
    }
}