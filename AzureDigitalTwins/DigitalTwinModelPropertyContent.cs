using Newtonsoft.Json;

namespace adt_auto_ingester.AzureDigitalTwins
{
    public class DigitalTwinModelPropertyContent : IDigitalTwinModelContent
    {
        [JsonProperty("@type")]
        public string Type {get;set;} = "Property";

        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("description", NullValueHandling = NullValueHandling.Ignore)]

        public string Description { get; set; }
        [JsonProperty("schema")]

        public string Schema { get; set; }
    }
}