// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Microsoft.Azure.Functions.Worker.Extensions.Abstractions;

namespace Microsoft.Azure.Functions.Worker.Extensions.MySql
{
    // The class to define MySql Output Attributes
    public class MySqlOutputAttribute : OutputBindingAttribute
    {
        /// <summary>
        /// Creates an instance of the <see cref="MySqlOutputAttribute"/>, which takes a list of rows and upserts them into the target table.
        /// </summary>
        /// <param name="commandText">The table name to upsert the values to.</param>
        /// <param name="connectionStringSetting">The name of the app setting where the MySql connection string is stored</param>
#pragma warning disable IDE0290
        public MySqlOutputAttribute(string commandText, string connectionStringSetting)
        {
            this.CommandText = commandText ?? throw new ArgumentNullException(nameof(commandText));
            this.ConnectionStringSetting = connectionStringSetting ?? throw new ArgumentNullException(nameof(connectionStringSetting));
        }
#pragma warning restore IDE0290

        /// <summary>
        /// The name of the app setting where the MySql connection string is stored
        /// (see https://dev.mysql.com/doc/dev/connector-net/latest/api/data_api/MySql.Data.MySqlClient.MySqlConnection.html).
        /// The attributes specified in the connection string are listed here
        /// https://dev.mysql.com/doc/dev/connector-net/latest/api/data_api/MySql.Data.MySqlClient.MySqlConnection.html#MySql_Data_MySqlClient_MySqlConnection__ctor_System_String_
        /// </summary>
        public string ConnectionStringSetting { get; }

        /// <summary>
        /// The table name to upsert the values to.
        /// </summary>
        public string CommandText { get; }
    }
}
