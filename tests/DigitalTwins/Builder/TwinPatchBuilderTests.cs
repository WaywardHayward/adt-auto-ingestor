using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Azure.DigitalTwins.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using src.AzureDigitalTwins.Builder;
using Xunit;

namespace adt_auto_ingester_tests.DigitalTwins.Builder
{
    public class TwinPatchBuilderTests : BaseTest
    {


        [Theory]

        [InlineData("{\"key\":\"something\", \"number\":-12.1, \"data\":\"2022-02-22T22:22:22.222Z\"}", "modelId", true)]
        public void GivenAJsonMessage_AValidPatch_IsCreated(string message, string modelId, bool twinHasAllProperties)
        {
            var builder = new TwinPatchBuilder();

            var configuration = GetConfigurationWith(string.Empty);
            var loggerFactory = new LoggerFactory();
            var ingestionContext = GetIngestionContext(configuration, loggerFactory);

            var messageObject = JObject.Parse(message);
            var twin = new BasicDigitalTwin();
            var sourceTime = DateTime.UtcNow.ToString("o");

            var expectedJson = new StringBuilder();

            expectedJson.Append("[");

            expectedJson.Append($"{{\"op\":\"replace\",\"path\":\"/$metadata/$model\",\"value\":\"{modelId}\"}},");

            var properties = messageObject.Properties();

            for (var propertyIndex = 0; propertyIndex < properties.Count(); propertyIndex++)
            {
                var property = properties.ElementAt(propertyIndex);
                 if (twinHasAllProperties)
                    twin.Contents.Add(property.Name, "existing value");                
                var valueString = property.Value is JValue ? ((JValue)property.Value).ToString(CultureInfo.InvariantCulture) : property.Value.ToString();
                expectedJson.Append($"{{\"op\":\"{(twinHasAllProperties ? "replace":"add")}\",\"path\":\"/{property.Name}\",\"value\":\"{valueString}\"}},");
                expectedJson.Append($"{{\"op\":\"replace\",\"path\":\"/$metadata/{property.Name}/sourceTime\",\"value\":\"{sourceTime}\"}}");
                if(propertyIndex < properties.Count() - 1)
                    expectedJson.Append(",");
            }

            expectedJson.Append("]");

            var patch = builder.Build(MessageContextFromMessage(messageObject, ingestionContext), modelId, twin, sourceTime);


            Assert.Equal(expectedJson.ToString(), patch.ToString());
        }


    }
}