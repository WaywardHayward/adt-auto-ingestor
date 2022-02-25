using Newtonsoft.Json.Linq;
using Azure.DigitalTwins.Core;
using Azure;
using System.Globalization;
using adt_auto_ingester.Ingestion;

namespace src.AzureDigitalTwins.Builder
{
    public class TwinPatchBuilder
    {


        public JsonPatchDocument Build(MessageContext context, string modelId, BasicDigitalTwin twin, string sourceTimestamp)
        {
            var patch = new JsonPatchDocument();

            patch.AppendReplace("/$metadata/$model", modelId);

            var propertyKeys = context.MessageProperties.Value.Keys;
      

            foreach (var property in propertyKeys)
            {
                var value = context.Message.SelectToken(property);
                
                var stringValue = value is JValue ? ((JValue)value).ToString(CultureInfo.InvariantCulture) : value.ToString();

                if (twin.Contents.ContainsKey(property))
                    patch.AppendReplace("/" + property, stringValue);
                else
                    patch.AppendAdd("/" + property, stringValue);

                patch.AppendReplace($"/$metadata/{property}/sourceTime", sourceTimestamp);
            }

            return patch;
        }


    }
}