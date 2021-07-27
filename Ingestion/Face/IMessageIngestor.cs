// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json.Linq;

namespace adt_auto_ingester.Ingestion.Face
{
    public interface IMessageIngestor
    {
        Task Ingest(EventData eventData, JObject message);
    }
}