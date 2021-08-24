// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingester.Ingestion.Generic;
using adt_auto_ingester.Ingestion.OPC;
using adt_auto_ingester.Ingestion.TwinIQ;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;


namespace adt_auto_ingester.Ingestion
{
    public class MessageIngestorFactory
    {
        private readonly IConfiguration _config;
        private bool _ingestOPC;
        private bool _ingestTIQ;
        private bool _ingestGeneric;

        public MessageIngestorFactory(IConfiguration config)
        {
            _config = config;

            if (Boolean.TryParse(config["INGESTION_TIQ_ENABLED"], out var tiqIngestEnabled))
                _ingestTIQ = tiqIngestEnabled;

            if (Boolean.TryParse(config["INGESTION_OPC_ENABLED"], out var opcIngestEnabled))
                _ingestOPC = opcIngestEnabled;

            if (Boolean.TryParse(config["INGESTION_GENERIC_ENABLED"], out var genIngestEnabled))
                _ingestGeneric = genIngestEnabled;


        }

        public IMessageIngestor Build(IngestionContext context, JObject message)
        {
            var messagePayLoad = message.SelectToken("message");

            if (messagePayLoad != null)
                message = messagePayLoad as JObject;

            context.Log.LogDebug(message.ToString());

            if (ShouldIngestTwinIQ(context,message))
                return new TwinIqMessageIngestor(context);
            
            if (ShouldIngestOPC(context,message))
                return new OpcMessageIngestor(context);
            
            if(ShouldIngestGeneric(context,message))
                return new GenericMessageIngestor(context);
            
            return null;
        }

        private bool ShouldIngestGeneric(IngestionContext context, JObject message)
        {
            context.Log.LogDebug($"Generic Ingestion {(_ingestGeneric ? "On": "Off")}");
            return _ingestGeneric;
        }

        private bool ShouldIngestTwinIQ(IngestionContext context,JObject message)
        {
            var shouldIngestTwinIq = _ingestTIQ 
            && message.ContainsKey("Routing") 
            && message.SelectToken("Routing.MessageType",false)?.Value<string>() == "tiq-ingest-telemetry" 
            && message.SelectToken("Routing.TiqTwin.Enabled", false)?.Value<bool>() == true;
            context.Log.LogDebug($"Twin IQ Ingestion {(_ingestTIQ ? "On": "Off")}");
            if(_ingestTIQ)
                context.Log.LogDebug($"Message {(shouldIngestTwinIq ? string.Empty: "Not")} identified for Twin IQ Ingestion");        
            return shouldIngestTwinIq;
        }

        private bool ShouldIngestOPC(IngestionContext context,JObject message)
        {
            var shouldIngestOPC = _ingestOPC && message.ContainsKey("NodeId") && message.ContainsKey("ApplicationUri") && message.ContainsKey("Value");
            context.Log.LogDebug($"OPC Ingestion {(_ingestOPC ? "On": "Off")}");
            if(_ingestOPC)
                context.Log.LogDebug($"Message {(shouldIngestOPC ? string.Empty: "Not")} identified for OPC Ingestion");
            return shouldIngestOPC;
        }

    }
}