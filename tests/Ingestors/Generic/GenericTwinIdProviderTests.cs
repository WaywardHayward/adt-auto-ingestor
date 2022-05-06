using System;
using System.Text;
using adt_auto_ingester.Ingestion;
using adt_auto_ingester.Ingestion.Generic;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Xunit;
using static Microsoft.Azure.EventHubs.EventData;

namespace adt_auto_ingester_tests.Ingestors.Generic
{
    public class GenericTwinIdProviderTests : BaseTest
    {


        [Theory]
        [InlineData("deviceId;DeviceId", "DeviceId", true)]
        [InlineData("deviceId", "deviceId", true)]
        [InlineData("device/deviceId", "device/deviceId", true)]
        [InlineData("messageId", "messageId", true)]
        [InlineData("message-Id", "messageId", false)]
        public void GivenAMessage_ReturnsTheExpectedTokenValue(string identifierPaths, string pathWithId, bool expectMatch)
        {
            var configuration = GetConfigurationWith(identifierPaths);
            var loggerFactory = new LoggerFactory();
            var ingestionContext = GetIngestionContext(configuration, loggerFactory);
            var deviceId = "MyDeviceId";
            var message = new JObject()
            {
                [pathWithId ?? "notSpecified"] = deviceId
            };

            ingestionContext.SetIngestionMessage(new EventData(Encoding.UTF8.GetBytes(message.ToString())));
            var messageContext = MessageContext.FromIngestionContext(ingestionContext, message);
            var twinIdProvider = new GenericMessageTwinIdProvider(configuration, new Logger<GenericMessageTwinIdProvider>(loggerFactory));
            var result = twinIdProvider.PopulateTwinId(messageContext);

            if (expectMatch)
                Assert.Equal(deviceId, result);
            else
                Assert.NotEqual(deviceId, result);
        }


        [Fact]
        public void GivenAMessageWithSystemPropertyIoTDeviceId_WhenNoOtherPropertiesArePresent_TheIoTDeviceIdIsUsed()
        {
            var iotDeviceId = "iot_device_id";
            var messageDeviceId = "message-device-id";
            var configuration = GetConfigurationWith(string.Empty);

            var loggerFactory = new LoggerFactory();
            var ingestionContext = GetIngestionContext(configuration, loggerFactory);
            var message = new JObject()
            {
                ["messageDeviceId"] = messageDeviceId
            };

            var messageContext = MessageContextFromMessage(message, ingestionContext, new SystemPropertiesCollection(0,DateTime.UtcNow, "0","0")
            {
                { "iothub-connection-device-id", iotDeviceId }
            });
            var twinIdProvider = new GenericMessageTwinIdProvider(configuration, new Logger<GenericMessageTwinIdProvider>(loggerFactory));
            var result = twinIdProvider.PopulateTwinId(messageContext);

            Assert.NotEqual(messageDeviceId, result);
            Assert.Equal(iotDeviceId, result);
        }


    }
}
