// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

[assembly: FunctionsStartup(typeof(Microsoft.Azure.WebJobs.Extensions.MySql.Tests.Integration.Startup))]

namespace Microsoft.Azure.WebJobs.Extensions.MySql.Tests.Integration
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
#pragma warning disable CA2327 // Test code, safe to ignore
            // Set default settings for JsonConvert to simulate a user doing the same in their function.
            // This will cause test failures if serialization/deserialization isn't done correctly
            // (using the helper methods in Utils.cs)
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings
            {
                Formatting = Formatting.Indented,
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            };
#pragma warning restore CA2327
        }
    }
}

