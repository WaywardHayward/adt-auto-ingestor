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
        public async Task Run([EventHubTrigger("%INGESTION_EVENTHUB_NAME%", Connection = "INGESTION_EVENTHUB_CONNECTION_STRING", ConsumerGroup ="%INGESTION_EVENTHUB_CONSUMERGROUP%")] EventData[] events, ILogger log)
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
                    if(messageBody.StartsWith("["))
                        messages = JArray.Parse(messageBody);
                    else
                        messages.Add(JObject.Parse(messageBody));

                    foreach(var message in messages){
                        var item = message as JObject;
                        var ingestor = MessageIngestorFactory.Build(context, item);
                        await ingestor.Ingest(eventData, item);
                    }

                    

                    // var twinId = GetTwinIdFromMessage(eventData, message);

                    // log.LogInformation("Checking For Twin Id in Event");

                    // if (string.IsNullOrWhiteSpace(twinId))
                    // {
                    //     log.LogWarning($"Message {messageBody} has no deviceId ");
                    //     continue;
                    // }

                    // log.LogInformation($"Found Twin Id {twinId} in Event");

                    // await WriteToTwin(log, adtUrl, client, message, twinId, await EnsureModelExists(log, adtUrl, client, message, twinId));

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

        private async Task<List<DigitalTwinModel>> GetAllModels(DigitalTwinsClient client, ILogger log)
        {
            var models = new List<DigitalTwinModel>();
            var modelsQuery = client.GetModelsAsync(new GetModelsOptions { IncludeModelDefinition = true });
            await foreach (var model in modelsQuery)
            {
                models.Add(JsonConvert.DeserializeObject<DigitalTwinModel>(model.DtdlModel));
            }
            return models;
        }

        private async Task<string> EnsureModelExists(ILogger log, string adtUrl, DigitalTwinsClient client, JObject message, string twinId, string modelId = "")
        {
            var rawModelId = $"dtmi:com:microsoft:autoingest:{_configuration["INGESTION_EVENTHUB_NAME"].Replace("-", string.Empty).ToLower()}:{twinId.Replace("-", string.Empty).ToLower()}";

            if (string.IsNullOrWhiteSpace(modelId))
                modelId = $"{rawModelId};1";

            var models = await GetAllModels(client, log);
            var modelExists = models.Any(m => m.Id.Split(";")[0] == rawModelId);
            var model = modelExists ? models.Where(m => m.Id.Split(";")[0] == rawModelId).OrderByDescending(m => int.Parse(m.Id.Split(";")[1])).FirstOrDefault() : null;

            if (model == null)
            {
                log.LogInformation($"Did Not Find Auto Model {modelId} in {adtUrl}");
                await CreateAutoIngestModel(log, adtUrl, client, message, twinId, modelId, model);
            }
            else
            {
                modelId = model.Id;
                log.LogInformation($"Model found for {modelId} {JsonConvert.SerializeObject(model)}");

                var messagePayLoad = message.SelectToken("message");

                if (messagePayLoad != null)
                    message = messagePayLoad as JObject;

                log.LogInformation("Checking Properties Exist in Model...");

                var modelContent = JsonConvert.DeserializeObject<DigitalTwinModel>(JsonConvert.SerializeObject(model));
                var modelVersion = int.Parse(model.Id.Split(";").LastOrDefault());
                var missingProperties = message.Properties().Where(p => modelContent.Contents.All(c => c.Name != p.Name));

                if (missingProperties.Any())
                {


                    log.LogInformation($"Found {missingProperties.Count()} missing Properties");

                    modelVersion++;

                    foreach (var missingProperty in missingProperties)
                        modelContent.Contents.Add(new DigitalTwinModelPropertyContent
                        {
                            Name = missingProperty.Name,
                            Schema = "string",
                            Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
                        });


                    modelId = $"{rawModelId};{modelVersion}";
                    modelContent.Id = modelId;
                    var modelDtdl = JsonConvert.SerializeObject(modelContent, Formatting.Indented);

                    log.LogInformation($"Creating Auto Model {modelId} in {adtUrl}\n: {modelDtdl}");
                    await client.CreateModelsAsync(new string[] { modelDtdl });
                }
                else
                {
                    log.LogInformation("No Properties missing");
                }


            }

            return modelId;
        }

        private static async Task CreateAutoIngestModel(ILogger log, string adtUrl, DigitalTwinsClient client, JObject message, string twinId, string modelId, DigitalTwinModel model)
        {

            var newModel = new DigitalTwinModel
            {
                Id = modelId,
                DisplayName = $"{twinId} Auto Provisioned Model"
            };

            var messagePayLoad = message.SelectToken("message");

            if (messagePayLoad != null)
                message = messagePayLoad as JObject;

            foreach (var property in message.Properties())
            {
                newModel.Contents.Add(new DigitalTwinModelPropertyContent
                {
                    Name = property.Name,
                    Schema = "string",
                    Description = $"Auto Provisioned Property at {DateTime.UtcNow}"
                });
            }

            var modelDtdl = JsonConvert.SerializeObject(newModel, Formatting.Indented);
            log.LogInformation($"Creating Auto Model {modelId} in {adtUrl}\n: {modelDtdl}");
            var models = await client.CreateModelsAsync(new string[] { modelDtdl });

            if (models.Value.Any())
                log.LogInformation($"Created Auto Model {modelId} in {adtUrl}");

        }

        private static async Task WriteToTwin(ILogger log, string adtUrl, DigitalTwinsClient client, JObject message, string twinId, string modelId)
        {
            var twin = await GetTwin(log, client, twinId);

            var messagePayLoad = message.SelectToken("message");

            if (messagePayLoad != null)
            {
                log.LogInformation($"Found message payload {message["message"].ToString()}");
                message = messagePayLoad.Value<JObject>();
            }


            if (twin == null)
            {
                await CreateNewTwin(log, adtUrl, client, message, twinId, modelId);
            }
            else
            {
                await UpdateExistingTwin(client, message, twinId, modelId, twin);
            }

            log.LogInformation($"Twin Data Applied to  {twinId} in {adtUrl}");
        }

        private static async Task UpdateExistingTwin(DigitalTwinsClient client, JObject message, string twinId, string modelId, BasicDigitalTwin twin)
        {
            var patch = new JsonPatchDocument();

            patch.AppendReplace("/$metadata/$model", modelId);

            foreach (var property in message?.Properties())
            {
                if (twin.Contents.Any(c => c.Key == property.Name))
                    patch.AppendReplace("/" + property.Name, message.SelectToken(property.Name).ToString());
                else
                    patch.AppendAdd("/" + property.Name, message.SelectToken(property.Name).ToString());
            }

            await client.UpdateDigitalTwinAsync(twinId, patch, twin.ETag);
        }

        private static async Task CreateNewTwin(ILogger log, string adtUrl, DigitalTwinsClient client, JObject message, string twinId, string modelId)
        {
            var newTwin = new BasicDigitalTwin()
            {
                Id = twinId,
                Metadata = new DigitalTwinMetadata
                {
                    ModelId = modelId
                }
            };

            foreach (var property in message?.Properties())
                newTwin.Contents.Add(property.Name, message.SelectToken(property.Name).ToString());

            log.LogInformation($"Updating or Creating Twin {twinId} in {adtUrl}");

            await client.CreateOrReplaceDigitalTwinAsync(twinId, newTwin);
        }

        private static async Task<BasicDigitalTwin> GetTwin(ILogger log, DigitalTwinsClient client, string twinId)
        {
            try
            {
                return await client.GetDigitalTwinAsync<BasicDigitalTwin>(twinId);
            }
            catch (Exception ex)
            {
                log.LogInformation($"Twin with Id {twinId} does not exist");
            }
            return null;
        }

        private static string GetTwinIdFromMessage(EventData eventData, JObject message)
        {
            var twinId = string.Empty;

            var deviceId = message.SelectToken("message.DeviceId", false);

            if (deviceId != null)
                twinId = deviceId.Value<string>();
            else
                twinId = eventData.SystemProperties.ContainsKey("iothub-connection-device-id") ? eventData.SystemProperties["iothub-connection-device-id"].ToString() : string.Empty;

            return twinId;
        }
    }
}
