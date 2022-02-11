// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

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

        [JsonIgnore]
        public Lazy<Dictionary<string, DigitalTwinModelPropertyContent>> PropertyContents => new Lazy<Dictionary<string, DigitalTwinModelPropertyContent>>(() => Contents.ToDictionary(p => p.Name, p => p));

        [JsonIgnore]
        public Lazy<string> ModelId => new Lazy<string>(() => Id.Split(";")[0]);

        [JsonIgnore]
        public Lazy<int> ModelVersion => new Lazy<int>(() => int.Parse(Id.Split(";")[1]));
        
    }
}