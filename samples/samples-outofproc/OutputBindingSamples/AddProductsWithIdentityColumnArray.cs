// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.MySql;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.MySql.SamplesOutOfProc.Common;

namespace Microsoft.Azure.WebJobs.Extensions.MySql.SamplesOutOfProc.OutputBindingSamples
{
    public static class AddProductsWithIdentityColumnArray
    {
        /// <summary>
        /// This shows an example of a MySql Output binding where the target table has a primary key
        /// which is an identity column. In such a case the primary key is not required to be in
        /// the object used by the binding - it will insert a row with the other values and the
        /// ID will be generated upon insert.
        /// </summary>
        /// <param name="req">The original request that triggered the function</param>
        /// <returns>The new product objects that will be upserted</returns>
        [Function(nameof(AddProductsWithIdentityColumnArray))]
        [MySqlOutput("ProductsWithIdentity", "MySqlConnectionString")]
        public static ProductWithoutId[] Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]
            HttpRequestData req)
        {
#pragma warning disable IDE0300
            ProductWithoutId[] products = new[]
            {
                new ProductWithoutId
                {
                    Name = "Cup",
                    Cost = 2
                },
                new ProductWithoutId
                {
                    Name = "Glasses",
                    Cost = 12
                }
            };
#pragma warning restore IDE0300
            return products;
        }
    }
}
