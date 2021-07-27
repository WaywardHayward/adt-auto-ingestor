// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Azure.DigitalTwins.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace adt_auto_ingester.Ingestion
{
    public class IngestionContext
    {
        public string AdtUrl {get;set;}
        public ILogger Log {get;set;}
        public List<Exception> Exceptions {get;set;} = new List<Exception>();
        public DigitalTwinsClient DigitalTwinsClient {get;set;}
        public IConfiguration Configuration {get;set;}
    }
}