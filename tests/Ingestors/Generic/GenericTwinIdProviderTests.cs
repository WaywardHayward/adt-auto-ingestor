using System;
using System.Collections.Generic;
using System.Text;
using adt_auto_ingester.AzureDigitalTwins.Face;
using adt_auto_ingester.Ingestion;
using adt_auto_ingester.Ingestion.Generic;
using adt_auto_ingester.Models;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace tests.Ingestors.Generic
{
    public class GenericTwinIdProviderTests
    {
        private string _twinInstance = "https://bing.com/";



        private IDigitalTwinsClientProvider GetDigitalTwinsClientProvider()
        {
            var mock = new Mock<IDigitalTwinsClientProvider>();
            mock.Setup(m => m.GetClient()).Returns(new DigitalTwinsClient(new Uri(_twinInstance), new DefaultAzureCredential()));
            return mock.Object;
        }

        private IngestionContext GetIngestionContext(IConfiguration configuration, LoggerFactory loggerFactory)
        {
            var ingestionContext = new IngestionContext(new Logger<IngestionContext>(loggerFactory), configuration, GetDigitalTwinsClientProvider());           
            return ingestionContext;
        }

        [Theory]
        [InlineData("deviceId;DeviceId", "DeviceId", true)]
        [InlineData("deviceId", "deviceId", true)]
        [InlineData("device/deviceId", "device/deviceId", true)]
        [InlineData("messageId", "messageId", true)]
        [InlineData("message-Id", "messageId", false)]
        public void GivenAMessage_ReturnsTheExpectedTokenValue(string identifierPaths, string pathWithId, bool expectMatch)
        {
            
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(new[]
            {
                new KeyValuePair<string, string>(Constants.INGESTION_ADT_TWIN_IDENTIFIERS, identifierPaths)
            }).Build();

            var loggerFactory = new LoggerFactory();
            var ingestionContext = GetIngestionContext(configuration, loggerFactory);
            var deviceId = "MyDeviceId";
            var message = new JObject() {
                [pathWithId ?? "notSpecified"] = deviceId
            };

            ingestionContext.SetIngestionMessage(new EventData(Encoding.UTF8.GetBytes(message.ToString())));

            var messageContext = MessageContext.FromIngestionContext(ingestionContext, message);

            var twinIdProvider = new GenericMessageTwinIdProvider(configuration, new Logger<GenericMessageTwinIdProvider>(loggerFactory));

            var result = twinIdProvider.PopulateTwinId(messageContext);

            if(expectMatch)
                Assert.Equal(deviceId, result);
            else
                Assert.NotEqual(deviceId, result);
        }


    }
}