
using System;
using adt_auto_ingester;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.AzureDigitalTwins.Face;
using adt_auto_ingester.Ingestion;
using adt_auto_ingester.Ingestion.Generic;
using adt_auto_ingester.Ingestion.OPC;
using adt_auto_ingester.Ingestion.TwinIQ;
using adt_auto_ingestor.AzureDigitalTwins;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;


[assembly: FunctionsStartup(typeof(Startup))]
namespace adt_auto_ingester
{
    public class Startup : FunctionsStartup
    {
        public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
        {
            builder.ConfigurationBuilder.AddJsonFile("local.settings.json", true, true);
            builder.ConfigurationBuilder.AddEnvironmentVariables();
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {            
            builder.Services.AddLogging(s => s.AddConsole());
            builder.Services.AddSingleton<IDigitalTwinsClientProvider, DigitalTwinsClientProvider>();
            builder.Services.AddSingleton<DigitalTwinCache>();
            builder.Services.AddTransient<IngestionContext>();
            builder.Services.AddSingleton<GenericMessageTwinIdProvider>();
            builder.Services.AddSingleton<TiqTwinIdProvider>();
            builder.Services.AddSingleton<OpcMessageTwinIdProvider>();
            builder.Services.AddSingleton<GenericMessageIngestor>();
            builder.Services.AddSingleton<OpcMessageIngestor>();
            builder.Services.AddSingleton<TwinIqMessageIngestor>();
            builder.Services.AddSingleton<DigitalTwinModelCache>();
            builder.Services.AddSingleton<MessageIngestorFactory>();
        }
    }
}