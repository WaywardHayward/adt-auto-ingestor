
using System;
using adt_auto_ingester;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;

[assembly: FunctionsStartup(typeof(Startup))]
namespace adt_auto_ingester
{
    public class Startup : FunctionsStartup
    {
        public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
        {
            builder.ConfigurationBuilder.AddJsonFile("local.settings.json", true);
            builder.ConfigurationBuilder.AddEnvironmentVariables();
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {
        }
    }
}