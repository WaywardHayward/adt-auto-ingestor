using Newtonsoft.Json;
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace adt_auto_ingester.AzureDigitalTwins.Face
{
    public interface IDigitalTwinModelContent
    {
        [JsonProperty("@type")]

         public string Type {get;set;}

         [JsonProperty("name")]
         public string Name {get;set;}
         public string Description {get;set;}

         public dynamic Schema {get;set;}
    }
}