// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.AzureDigitalTwins.Face;
using adt_auto_ingester.Models;
using Azure.DigitalTwins.Core;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion
{
    public class IngestionContext
    {
        public string AdtUrl {get; private set;}
        public ILogger Log {get;private set;}
        public List<Exception> Exceptions {get;set;} = new List<Exception>();
        public DigitalTwinsClient DigitalTwinsClient {get; private set;}
        public IConfiguration Configuration {get; private set;}

        public EventData EventData {get; private set;}

        public IngestionContext(ILogger<IngestionContext> log, IConfiguration configuration, IDigitalTwinsClientProvider clientProvider){
            DigitalTwinsClient = clientProvider.GetClient();
            Configuration = configuration;
            AdtUrl = Configuration[Constants.ADT_URL_SETTING];
            Log = log;
        }

        public void SetIngestionMessage(EventData eventData)
        {
            EventData = eventData;
        }
    }
}