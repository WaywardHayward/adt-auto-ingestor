using adt_auto_ingester.Ingestion.Face;
using adt_auto_ingester.Ingestion.Generic;
using adt_auto_ingester.Ingestion.OPC;
using adt_auto_ingester.Ingestion.TwinIQ;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;


namespace adt_auto_ingester.Ingestion
{
    public static class MessageIngestorFactory
    {

            public static IMessageIngestor Build(IngestionContext context, JObject message){
                
                var messagePayLoad = message.SelectToken("message");

                if (messagePayLoad != null)
                    message = messagePayLoad as JObject;

                context.Log.LogInformation(message.ToString());

                if(message.ContainsKey("Routing"))
                    return new TwinIqMessageIngestor(context);
                if(message.ContainsKey("DataSetClassId"))
                    return new OpcMessageIngestor(context);
                return new GenericMessageIngestor(context);
            }


    }
}