using System;
using adt_auto_ingester.AzureDigitalTwins.Face;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Extensions.Configuration;

namespace adt_auto_ingester.AzureDigitalTwins
{

    public class DigitalTwinsClientProvider : IDigitalTwinsClientProvider
    {
        private readonly IConfiguration _config;

        public DigitalTwinsClientProvider(IConfiguration config)
        {
            _config = config;
        }

        public DigitalTwinsClient GetClient()
        {
            var client = new DigitalTwinsClient(new Uri(_config["INGESTION_TWIN_URL"]), new DefaultAzureCredential());
            return client;
        }
    }
}