using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.AzureDigitalTwins
{
    public class DigitalTwinModel
    {
        [JsonProperty("@id")]
        public string Id { get; set; }
        
        [JsonProperty("@type")]
        public string Type { get; set; } = "Interface";
        
        [JsonProperty("@context")]
        public string[] Context { get; set; } = new string []{ "dtmi:dtdl:context;2"};
        
        [JsonProperty("displayName")]
        public string DisplayName {get;set;}

        [JsonProperty("contents")]
        public List<DigitalTwinModelPropertyContent> Contents {get;set;}= new List<DigitalTwinModelPropertyContent>();
        
    }
}