// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using adt_auto_ingester.Ingestion;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Microsoft.Adt.AutoIngestor
{
    public class EventHubIngestor
    {

        private readonly IConfiguration _configuration;
        private readonly MessageIngestorFactory _ingestorFactory;
        private readonly IngestionContext _context;

        public EventHubIngestor(IConfiguration configuration, MessageIngestorFactory ingestorFactory, IngestionContext context)
        {
            _configuration = configuration;
            _ingestorFactory = ingestorFactory;
            _context = context;
        }

        [FunctionName("EventHubIngestor")]
        public async Task Run([EventHubTrigger("%INGESTION_EVENTHUB_NAME%", Connection = "INGESTION_EVENTHUB_CONNECTION_STRING", ConsumerGroup = "%INGESTION_EVENTHUB_CONSUMERGROUP%")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            log.LogInformation($"Messages Recieved {events.Length}");

            foreach (EventData eventData in events)
            {
                try
                {
                    _context.SetIngestionMessage(eventData);
                    var messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                                       
                    JArray messages = new JArray();
                    
                    if (messageBody.StartsWith("["))
                        messages = JArray.Parse(messageBody);
                    else
                        messages.Add(JObject.Parse(messageBody));

                    foreach (var message in messages)
                    {
                        var item = message as JObject;
                        var messageContext = MessageContext.FromIngestionContext(_context, item);
                        var ingestor = _ingestorFactory.Build(messageContext);

                        if(ingestor != null)
                        {
                            await ingestor?.Ingest(messageContext);
                        }
                        else
                        {
                            log.LogWarning("No ingestor for message - ignoring...");
                        }
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
