// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingester.Ingestion.Generic;
using adt_auto_ingester.Ingestion.OPC;
using adt_auto_ingester.Ingestion.TwinIQ;
using adt_auto_ingestor.AzureDigitalTwins;
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
        private DigitalTwinModelCache _modelCache;
        
        private Dictionary<string, IMessageIngestor> _ingestors;

        public MessageIngestorFactory(IConfiguration config, DigitalTwinModelCache modelCache, TwinIqMessageIngestor twinIqMessageIngestor, OpcMessageIngestor opcMessageIngestor, GenericMessageIngestor genericMessageIngestor)
        {
            _config = config;
            _modelCache = modelCache;

            if (Boolean.TryParse(config["INGESTION_TIQ_ENABLED"], out var tiqIngestEnabled))
                _ingestTIQ = tiqIngestEnabled;

            if (Boolean.TryParse(config["INGESTION_OPC_ENABLED"], out var opcIngestEnabled))
                _ingestOPC = opcIngestEnabled;

            if (Boolean.TryParse(config["INGESTION_GENERIC_ENABLED"], out var genIngestEnabled))
                _ingestGeneric = genIngestEnabled;
            
            _ingestors = new Dictionary<string, IMessageIngestor>(){
               { nameof(TwinIqMessageIngestor), twinIqMessageIngestor },
               { nameof(GenericMessageIngestor), genericMessageIngestor},
               { nameof(OpcMessageIngestor), opcMessageIngestor }

            };
        }

        public IMessageIngestor Build(MessageContext context)
        {

            if (ShouldIngestTwinIQ(context))
                return _ingestors[nameof(TwinIqMessageIngestor)];
            
            if (ShouldIngestOPC(context))
                return _ingestors[nameof(OpcMessageIngestor)];
            
            if(ShouldIngestGeneric(context))
                return _ingestors[nameof(GenericMessageIngestor)];
            
            return null;
        }

        private bool ShouldIngestGeneric(MessageContext context)
        {
            context.IngestionContext.Log.LogDebug($"Generic Ingestion {(_ingestGeneric ? "On": "Off")}");
            return _ingestGeneric;
        }

        private bool ShouldIngestTwinIQ(MessageContext context)
        {
            var shouldIngestTwinIq = _ingestTIQ 
            && context.Message.ContainsKey("Routing") 
            && context.Message.SelectToken("Routing.MessageType",false)?.Value<string>() == "tiq-ingest-telemetry" 
            && context.Message.SelectToken("Routing.TiqTwin.Enabled", false)?.Value<bool>() == true;
            context.IngestionContext.Log.LogDebug($"Twin IQ Ingestion {(_ingestTIQ ? "On": "Off")}");
            if(_ingestTIQ)
                context.IngestionContext.Log.LogDebug($"Message {(shouldIngestTwinIq ? string.Empty: "Not")} identified for Twin IQ Ingestion");        
            return shouldIngestTwinIq;
        }

        private bool ShouldIngestOPC(MessageContext context)
        {
            var shouldIngestOPC = _ingestOPC && context.Message.ContainsKey("NodeId") && context.Message.ContainsKey("ApplicationUri") && context.Message.ContainsKey("Value");
            context.IngestionContext.Log.LogDebug($"OPC Ingestion {(_ingestOPC ? "On": "Off")}");
            if(_ingestOPC)
                context.IngestionContext.Log.LogDebug($"Message {(shouldIngestOPC ? string.Empty: "Not")} identified for OPC Ingestion");
            return shouldIngestOPC;
        }

    }
}