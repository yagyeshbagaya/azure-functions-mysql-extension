// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

namespace Microsoft.Azure.Functions.Worker.Extensions.MySql
{
    /// <summary>
    /// Represents the changed row in the user table.
    /// </summary>
    /// <typeparam name="T">POCO class representing the row in the user table</typeparam>
    public sealed class MySqlChange<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlChange{T}"/> class.
        /// </summary>
        /// <param name="operation">Change operation</param>
        /// <param name="item">POCO representing the row in the user table on which the change operation took place</param>
#pragma warning disable IDE0290
        public MySqlChange(MySqlChangeOperation operation, T item)
        {
            this.Operation = operation;
            this.Item = item;
        }
#pragma warning restore IDE0290

        /// <summary>
        /// Change operation (insert or update).
        /// </summary>
        public MySqlChangeOperation Operation { get; }

        /// <summary>
        /// POCO representing the row in the user table on which the change operation took place.
        /// </summary>
        public T Item { get; }
    }

    /// <summary>
    /// Represents the type of change operation in the table row.
    /// </summary>
    public enum MySqlChangeOperation
    {
        Update
    }
}