// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.MySql.Samples.Common;

namespace Microsoft.Azure.WebJobs.Extensions.MySql.Samples.OutputBindingSamples
{
    public static class AddProductsArray
    {
        [FunctionName(nameof(AddProductsArray))]
        public static IActionResult Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "addproducts-array")]
            [FromBody] List<Product> products,
            [MySql("Products", "MySqlConnectionString")] out Product[] output)
        {
            // Upsert the products, which will insert them into the Products table if the primary key (ProductId) for that item doesn't exist. 
            // If it does then update it to have the new name and cost
#pragma warning disable IDE0305
            output = products.ToArray();
#pragma warning restore IDE0305

            return new CreatedResult($"/api/addproducts-array", output);
        }
    }
}
