// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Microsoft.Azure.Functions.Worker.Extensions.Abstractions;

namespace Microsoft.Azure.Functions.Worker.Extensions.MySql
{
    public sealed class MySqlTriggerAttribute : TriggerBindingAttribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlTriggerAttribute"/> class, which triggers the function when any changes on the specified table are detected.
        /// </summary>
        /// <param name="tableName">Name of the table to watch for changes.</param>
        /// <param name="connectionStringSetting">The name of the app setting where the MySQL connection string is stored</param>
        /// <param name="leasesTableName">Optional - The name of the table used to store leases. If not specified, the leases table name will be Leases_{FunctionId}_{TableId}</param>
#pragma warning disable IDE0290
        public MySqlTriggerAttribute(string tableName, string connectionStringSetting, string leasesTableName = null)
        {
            this.TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            this.ConnectionStringSetting = connectionStringSetting ?? throw new ArgumentNullException(nameof(connectionStringSetting));
            this.LeasesTableName = leasesTableName;
        }
#pragma warning restore IDE0290

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlTriggerAttribute"/> class with null value for LeasesTableName.
        /// </summary>
        /// <param name="tableName">Name of the table to watch for changes.</param>
        /// <param name="connectionStringSetting">The name of the app setting where the MySQL connection string is stored</param>
        public MySqlTriggerAttribute(string tableName, string connectionStringSetting) : this(tableName, connectionStringSetting, null) { }

        /// <summary>
        /// Name of the app setting containing the MySQL connection string.
        /// </summary>
        public string ConnectionStringSetting { get; }

        /// <summary>
        /// Name of the table to watch for changes.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Name of the table used to store leases.
        /// If not specified, the leases table name will be Leases_{FunctionId}_{TableId}
        /// </summary>
        public string LeasesTableName { get; }
    }
}