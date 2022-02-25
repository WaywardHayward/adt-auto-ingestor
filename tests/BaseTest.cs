using System;
using System.Collections.Generic;
using System.Text;
using adt_auto_ingester.AzureDigitalTwins.Face;
using adt_auto_ingester.Ingestion;
using adt_auto_ingester.Models;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json.Linq;
using static Microsoft.Azure.EventHubs.EventData;

namespace adt_auto_ingester_tests
{
    public abstract class BaseTest
    {
        protected string _twinInstance = "https://bing.com/";

        protected IDigitalTwinsClientProvider GetDigitalTwinsClientProvider()
        {
            var mock = new Mock<IDigitalTwinsClientProvider>();
            mock.Setup(m => m.GetClient()).Returns(new DigitalTwinsClient(new Uri(_twinInstance), new DefaultAzureCredential()));
            return mock.Object;
        }

        protected IngestionContext GetIngestionContext(IConfiguration configuration, LoggerFactory loggerFactory)
        {
            var ingestionContext = new IngestionContext(new Logger<IngestionContext>(loggerFactory), configuration, GetDigitalTwinsClientProvider());
            return ingestionContext;
        }

        protected IConfiguration GetConfigurationWith(string ingestionAdtTwinIdentifiers)
        {
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(new[]
            {
                new KeyValuePair<string, string>(Constants.INGESTION_ADT_TWIN_IDENTIFIERS, ingestionAdtTwinIdentifiers)
            }).Build();
            return configuration;
        }

        protected MessageContext MessageContextFromMessage(JObject message, IngestionContext context, SystemPropertiesCollection systemProperties = null){
           
            var eventData = new EventData(Encoding.UTF8.GetBytes(message.ToString()));

            if(systemProperties != null)
                eventData.SystemProperties = systemProperties;
                
            context.SetIngestionMessage(eventData);

            return  MessageContext.FromIngestionContext(context, message);
        }
    }
}