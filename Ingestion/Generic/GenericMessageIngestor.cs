// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingester.Models;
using adt_auto_ingestor.AzureDigitalTwins;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.Generic
{
    public class GenericMessageIngestor : AbstractMessageIngestor, IMessageIngestor
    {

        public GenericMessageIngestor(ILogger<GenericMessageIngestor> log, DigitalTwinModelCache modelCache, GenericMessageTwinIdProvider twinIdProvider) : base(modelCache, twinIdProvider, log)
        {

        }

        public async Task Ingest(MessageContext context)
        {
            try
            {
                var twinId = _twinIdProvider.PopulateTwinId(context);

                _logger.LogTrace("Checking For Twin Id in Event");

                if (string.IsNullOrWhiteSpace(twinId))
                {
                    _logger.LogWarning($"Message {context.Message.ToString()} has no deviceId ");
                    return;
                }

                if (context.Message.SelectToken("Payload.SensorId", false)?.Value<string>() == "Heartbeat" || context.Message.SelectToken("Payload.SensorId", false)?.Value<string>() == "ModelManager")
                {
                    _logger.LogTrace($"Ignoring Heartbeat");
                    return;
                }

                LogTimestampOffset(context);

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
            var modelId = GetModelId(context.Message, context.IngestionContext.Configuration[Constants.INGESTION_ADT_MODEL_IDENTIFIERS]?.Split(";")) ?? $"{context.IngestionContext.Configuration[Constants.INGESTION_EVENT_HUB_NAME].Replace("-", string.Empty).ToLower()}:{twinId.Replace("-", string.Empty).ToLower()}";
            var rawModelId = $"dtmi:com:microsoft:autoingest:{modelId}";
            return await EnsureModelExists(context, rawModelId, modelId ?? twinId.Split(":").LastOrDefault());
        }

        private string GetModelId(JObject message, string[] identifierPaths)
        {
            if (identifierPaths == null)
                return null;

            foreach (var path in identifierPaths)
            {
                var modelId = message.SelectToken(path, false)?.Value<string>();

                if (!string.IsNullOrWhiteSpace(modelId))
                {
                    _logger.LogInformation($"Found Model Id {modelId} in Message via Property Path {path}");
                    return modelId;
                }
            }
            return null;
        }

        private void LogTimestampOffset(MessageContext context)
        {
            var timestampIdentifiers = context.IngestionContext.Configuration[Constants.INGESTION_ADT_TIMESTAMP_IDENTIFIERS]?.Split(";");

            if (timestampIdentifiers == null || timestampIdentifiers.Length == 0)
            {
                _logger.LogInformation($"No Timestamp Identifiers Found in Configuration");
                return;
            }

            foreach (var timestampIdentifier in timestampIdentifiers)
            {
                var timestamp = context.Message.SelectToken(timestampIdentifier, false)?.Value<DateTime>();

                if (timestamp == null)
                {
                    _logger.LogInformation($"No Timestamp Found in Message via Property Path {timestampIdentifier}");
                    continue;
                }

                _logger.LogInformation($"Timestamp {timestamp} Offset {DateTime.UtcNow - timestamp}");


                break;
            }


        }
    }
}