
# Azure Digital Twin Auto Ingestor
> Function app which Auto ingests data from an Event Hub into Azure Digital Twins

A major issue when provisioning and populating azure digital twins is the up-front modelling and mapping required to ingest telemetry which you may already have hitting an event hub. 

This Function App can be configured to listen to an Event Hub and ingest messages landing on that event hub into a Digital Twins Instance.

## How to Run the Function Locally

To debug this function locally in vscode you will require the following items.

 - [Visual Studio Code](https://code.visualstudio.com/)
 - [Azure Tools Extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode.vscode-node-azure-pack)
 - [Azure Functions Core Tools](https://github.com/Azure/azure-functions-core-tools#installing)
 
  _ℹ️ You must have [Access to the Azure Digital Twins instance](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-set-up-instance-powershell#set-up-user-access-permissions "More on how to grand access to an Azure Digital Twins Instance can be found here.") you are running against __and__ have the role "Azure Digital Twins Data Owner" as a minimum to run this function app._

This function app uses dotnetcore 3.1 LTS to build and run it use the standard dotnet commands.

```sh
dotnet restore
```

```sh
dotnet build
```

This function app makes use of a local.settings.json file which you must populate with settings described in the [Settings](#Function_App_Settings) section below.

## Function App Settings

The following settings are required on top of the standard Azure Function app settings to run the function.

| Setting Name | Type | Description |
| ------------ | ---- | ----------- |
| INGESTION_EVENTHUB_CONNECTION_STRING | string | This is the connection string for the event hub you want to connect to |
| INGESTION_EVENTHUB_NAME | string | This is the name of the event hub you want to connect to |
| INGESTION_EVENTHUB_CONSUMERGROUP | string | The Consumer Group you want to use to listen to the event hub with |
| INGESTION_TWIN_URL | string | The fully qualified Azure Digital Twins instance url https://\<your-instance-name\>.api.\<your-instance-location-shortcode\>.digitaltwins.azure.net

  _ℹ️ The Azure Functiona App must run with a [Managed Identity](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview "Find out more about Azure Managed Identities Here") which has Access to the Azure Digital Twins instance you are running against __and__ have the role "Azure Digital Twins Data Owner" as a minimum to run this function app._

