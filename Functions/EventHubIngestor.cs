using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using adt_auto_ingester.AzureDigitalTwins;
using adt_auto_ingester.Ingestion;
using adt_auto_ingester.Models;
using Azure;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Adt.AutoIngestor
{
    public class EventHubIngestor
    {

        private readonly IConfiguration _configuration;

        public EventHubIngestor(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [FunctionName("EventHubIngestor")]
        public async Task Run([EventHubTrigger("%INGESTION_EVENTHUB_NAME%", Connection = "INGESTION_EVENTHUB_CONNECTION_STRING", ConsumerGroup = "%INGESTION_EVENTHUB_CONSUMERGROUP%")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();
            var adtUrl = _configuration[Constants.ADT_URL_SETTING];
            var client = new DigitalTwinsClient(new Uri(adtUrl), new ManagedIdentityCredential());

            var context = new IngestionContext
            {
                DigitalTwinsClient = client,
                AdtUrl = adtUrl,
                Log = log,
                Exceptions = exceptions,
                Configuration = _configuration
            };

            log.LogInformation($"Messages Recieved {events.Length}");

            foreach (EventData eventData in events)
            {
                try
                {
                    var messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    JArray messages = new JArray();
                    if (messageBody.StartsWith("["))
                        messages = JArray.Parse(messageBody);
                    else
                        messages.Add(JObject.Parse(messageBody));

                    foreach (var message in messages)
                    {
                        var item = message as JObject;
                        var ingestor = MessageIngestorFactory.Build(context, item);
                        await ingestor.Ingest(eventData, item);
                    }


                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();

        }
    }
}
