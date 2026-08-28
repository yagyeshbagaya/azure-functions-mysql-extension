// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Caching;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MoreLinq;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using static Microsoft.Azure.WebJobs.Extensions.MySql.MySqlBindingConstants;
using static Microsoft.Azure.WebJobs.Extensions.MySql.MySqlBindingUtilities;

namespace Microsoft.Azure.WebJobs.Extensions.MySql
{

    internal class PrimaryKey
    {
        public readonly string Name;

        public readonly bool IsAutoIncrement;

        public readonly bool HasDefault;

        public PrimaryKey(string name, bool isAutoIncrement, bool hasDefault)
        {
            this.Name = name;
            this.IsAutoIncrement = isAutoIncrement;
            this.HasDefault = hasDefault;
        }

        public override string ToString()
        {
            return this.Name;
        }
    }

    /// <typeparam name="T">A user-defined POCO that represents a row of the user's table</typeparam>
    internal class MySqlAsyncCollector<T> : IAsyncCollector<T>, IDisposable
    {
        private const string ColumnName = "COLUMN_NAME";
        private const string ColumnDefinition = "COLUMN_DEFINITION";

        private const string HasDefault = "has_default";
        private const string IsAutoIncrement = "is_autoincrement";

        private const int AZ_FUNC_TABLE_INFO_CACHE_TIMEOUT_MINUTES = 10;

        private readonly IConfiguration _configuration;
        private readonly MySqlAttribute _attribute;
        private readonly ILogger _logger;

        private readonly List<T> _rows = new List<T>();
        private readonly SemaphoreSlim _rowLock = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlAsyncCollector{T}"/> class.
        /// </summary>
        /// <param name="configuration">
        /// Contains the function's configuration properties
        /// </param>
        /// <param name="attribute">
        /// Contains as one of its attributes the MySQL table that rows will be inserted into
        /// </param>
        /// <param name="logger">
        /// Logger Factory for creating an ILogger
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown if either configuration or attribute is null
        /// </exception>
        public MySqlAsyncCollector(IConfiguration configuration, MySqlAttribute attribute, ILogger logger)
        {
            this._configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this._attribute = attribute ?? throw new ArgumentNullException(nameof(attribute));
            this._logger = logger;
            using (MySqlConnection connection = BuildConnection(attribute.ConnectionStringSetting, configuration))
            {
                AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(this.HandleException);
                connection.OpenAsyncWithMySqlErrorHandling(CancellationToken.None).Wait();
            }
        }

        public void HandleException(object sender, UnhandledExceptionEventArgs e)
        {
            this._logger.LogError($"Unable to establish connection with the provided connection string. Exception: {e.ExceptionObject}");
            Environment.Exit(0);
        }

        /// <summary>
        /// Adds an item to this collector that is processed in a batch along with all other items added via
        /// AddAsync when <see cref="FlushAsync"/> is called. Each item is interpreted as a row to be added to the MySQL table
        /// specified in the MySQL Binding.
        /// </summary>
        /// <param name="item"> The item to add to the collector </param>
        /// <param name="cancellationToken">The cancellationToken is not used in this method</param>
        /// <returns> A CompletedTask if executed successfully </returns>
        public async Task AddAsync(T item, CancellationToken cancellationToken = default)
        {
            if (item != null)
            {
                await this._rowLock.WaitAsync(cancellationToken);
                try
                {
                    this._rows.Add(item);
                }
                finally
                {
                    this._rowLock.Release();
                }
            }
        }

        /// <summary>
        /// Processes all items added to the collector via <see cref="AddAsync"/>. Each item is interpreted as a row to be added
        /// to the MySQL table specified in the MySQL Binding. All rows are added in one transaction. Nothing is done
        /// if no items were added via AddAsync.
        /// </summary>
        /// <param name="cancellationToken">The cancellationToken is not used in this method</param>
        /// <returns> A CompletedTask if executed successfully. If no rows were added, this is returned
        /// automatically. </returns>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            await this._rowLock.WaitAsync(cancellationToken);
            try
            {
                if (this._rows.Count != 0)
                {
                    this._logger.LogInformation($"Sending event Flush Async");
                    await this.UpsertRowsAsync(this._rows, this._attribute, this._configuration);
                    this._rows.Clear();
                }
            }
            catch (Exception ex)
            {
                this._logger.LogError($"Error sending event Flush Async. Message={ex.Message}");
                throw;
            }
            finally
            {
                this._rowLock.Release();
            }
        }

        /// <summary>
        /// Upserts the rows specified in "rows" to the table specified in "attribute"
        /// If a primary key in "rows" already exists in the table, the row is interpreted as an update rather than an insert.
        /// The column values associated with that primary key in the table are updated to have the values specified in "rows".
        /// If a new primary key is encountered in "rows", the row is simply inserted into the table.
        /// </summary>
        /// <param name="rows"> The rows to be upserted </param>
        /// <param name="attribute"> Contains the name of the table to be modified and MySQL connection information </param>
        /// <param name="configuration"> Used to build up the connection </param>
        private async Task UpsertRowsAsync(IList<T> rows, MySqlAttribute attribute, IConfiguration configuration)
        {
            using (MySqlConnection connection = BuildConnection(attribute.ConnectionStringSetting, configuration))
            {
                await connection.OpenAsync();
                string fullTableName = attribute.CommandText;

                // Include the connection string hash as part of the key in case this customer has the same table in two different MySql Servers
                string cacheKey = $"{connection.ConnectionString.GetHashCode()}-{fullTableName}";

                ObjectCache cachedTables = MemoryCache.Default;

                int timeout = AZ_FUNC_TABLE_INFO_CACHE_TIMEOUT_MINUTES;
                string timeoutEnvVar = Environment.GetEnvironmentVariable("AZ_FUNC_TABLE_INFO_CACHE_TIMEOUT_MINUTES");
                if (!string.IsNullOrEmpty(timeoutEnvVar))
                {
                    if (int.TryParse(timeoutEnvVar, NumberStyles.Integer, CultureInfo.InvariantCulture, out timeout))
                    {
                        this._logger.LogDebug($"Overriding default table info cache timeout with new value {timeout}");
                    }
                    else
                    {
                        timeout = AZ_FUNC_TABLE_INFO_CACHE_TIMEOUT_MINUTES;
                    }
                }

                if (!(cachedTables[cacheKey] is TableInformation tableInfo))
                {
                    this._logger.LogInformation($"Sending event TableInfoCacheMiss");
                    // set the columnNames for supporting T as JObject since it doesn't have columns in the member info.
                    tableInfo = TableInformation.RetrieveTableInformation(connection, fullTableName, this._logger, GetColumnNamesFromItem(rows.First()));
                    var policy = new CacheItemPolicy
                    {
                        // Re-look up the primary key(s) after timeout (default timeout is 10 minutes)
                        AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(timeout)
                    };

                    cachedTables.Set(cacheKey, tableInfo, policy);
                }
                else
                {
                    this._logger.LogInformation($"Sending event TableInfoCacheHit");
                }

                IEnumerable<string> extraProperties = GetExtraProperties(tableInfo.Columns, rows.First());
                if (extraProperties.Any())
                {
                    string message = $"The following properties in {typeof(T)} do not exist in the specified table";
                    var ex = new InvalidOperationException(message);
                    throw ex;
                }

                IEnumerable<string> columnNamesFromItem = GetColumnNamesFromItem(rows.First());

                var table = new MySqlObject(fullTableName);
                string insertQuery = TableInformation.GetInsertQuery(table, columnNamesFromItem);

                string duplicateUpdateQuery = TableInformation.GetOnDuplicateUpdateQuery(columnNamesFromItem);

                var transactionSw = Stopwatch.StartNew();
                int batchSize = 1000;
                MySqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.Connection = connection;
                    command.Transaction = transaction;
                    int batchCount = 0;
                    var commandSw = Stopwatch.StartNew();
                    foreach (IEnumerable<T> batch in rows.Batch(batchSize))
                    {
                        batchCount++;
                        var parameters = new List<MySqlParameter>();
                        GenerateDataQueryForMerge(tableInfo, batch, columnNamesFromItem, out string newDataQuery, parameters);
                        command.CommandText = $"{insertQuery} {newDataQuery} {duplicateUpdateQuery};";
                        command.Parameters.Clear();
                        command.Parameters.AddRange(parameters.ToArray());

                        await command.ExecuteNonQueryAsyncWithLogging(this._logger, CancellationToken.None, true);
                    }
                    transaction.Commit();
                    transactionSw.Stop();
                    this._logger.LogInformation($"Sending event Upsert Rows - BatchCount: {batchCount}, TransactionDurationMs: {transactionSw.ElapsedMilliseconds}," +
                        $"CommandDurationMs: {commandSw.ElapsedMilliseconds}, BatchSize: {batchSize}, Rows: {rows.Count}");
                }
                catch (Exception ex)
                {
                    try
                    {
                        this._logger.LogError($"Error Upserting rows. Message:{ex.Message}");
                        transaction.Rollback();
                    }
                    catch (Exception ex2)
                    {
                        this._logger.LogError($"Error Upserting rows and rollback. Message:{ex2.Message}");
                        string message2 = $"Encountered exception during upsert and rollback.";
                        throw new AggregateException(message2, new List<Exception> { ex, ex2 });
                    }
                    throw new InvalidOperationException($"Unexpected error upserting rows", ex);
                }
            }
        }

        /// <summary>
        /// Checks if any properties in T do not exist as columns in the table
        /// to upsert to and returns the extra property names in a List.
        /// </summary>
        /// <param name="columns"> The columns of the table to upsert to </param>
        /// <param name="rowItem"> Sample row used to get the column names when item is a JObject </param>
        /// <returns>List of property names that don't exist in the table</returns>
        private static IEnumerable<string> GetExtraProperties(IDictionary<string, string> columns, T rowItem)
        {
            if (typeof(T) == typeof(JObject))
            {
                Dictionary<string, string> dictObj = (rowItem as JObject).ToObject<Dictionary<string, string>>();
                return dictObj.Keys.Where(prop => !columns.ContainsKey(MySqlObject.GetMySqlColNameFormatted(prop)))
                .Select(prop => prop);
            }
            return typeof(T).GetProperties().ToList()
                .Where(prop => !columns.ContainsKey(MySqlObject.GetMySqlColNameFormatted(prop.Name)))
                .Select(prop => prop.Name);
        }
        /// <summary>
        /// Gets the column names from PropertyInfo when T is POCO
        /// and when T is JObject, parses the data to get column names
        /// </summary>
        /// <param name="row"> Sample row used to get the column names when item is a JObject </param>
        /// <returns>List of column names in the table</returns>
        private static IEnumerable<string> GetColumnNamesFromItem(T row)
        {
            if (typeof(T) == typeof(JObject))
            {
                var jsonObj = JObject.Parse(row.ToString());
                Dictionary<string, string> dictObj = jsonObj.ToObject<Dictionary<string, string>>();
                return dictObj.Keys;
            }
            return typeof(T).GetProperties().Select(prop => prop.Name);
        }

        private static string GetColValuesForUpsert(T row, TableInformation table, IEnumerable<string> columnNamesFromItem, IList<MySqlParameter> parameters)
        {
            //build a string of column data
            string jsonRowDataInString = Utils.JsonSerializeObject(row, table.JsonSerializerSettings);
            var jsonRowData = JObject.Parse(jsonRowDataInString);

            //to store the parameter placeholders for the values of each property in each row
            var colValuePlaceholders = new List<string>();

            foreach (string colName in columnNamesFromItem)
            {
                //find the col value in jsonRowData, to the respecting column name
                JToken token = jsonRowData[colName];
                string colVal = token?.ToString();

                // Treat null/empty values as SQL NULL (preserves previous behavior); otherwise bind the
                // value as a parameter so attacker-controlled data can never be executed as SQL (CWE-89).
                object parameterValue = string.IsNullOrEmpty(colVal) ? DBNull.Value : token.ToObject<object>();

                string parameterName = $"@p{parameters.Count}";
                parameters.Add(new MySqlParameter(parameterName, parameterValue));
                colValuePlaceholders.Add(parameterName);
            }
            string joinedColValues = '(' + string.Join(", ", colValuePlaceholders) + ")";
            return joinedColValues;
        }

        /// <summary>
        /// Generates T-MySQL for data to be upserted using Merge.
        /// This needs to be regenerated for every batch to upsert.
        /// </summary>
        /// <param name="table">Information about the table we will be upserting into</param>
        /// <param name="rows">Rows to be upserted</param>
        /// <param name="columnNamesFromItem">column list to be upserted</param>
        /// <param name="newDataQuery">Generated T-MySQL data query</param>
        /// <param name="parameters">Collection that receives one parameter per upserted value</param>
        /// <returns>T-MySQL containing data for merge</returns>
        private static void GenerateDataQueryForMerge(TableInformation table, IEnumerable<T> rows, IEnumerable<string> columnNamesFromItem, out string newDataQuery, IList<MySqlParameter> parameters)
        {
            // to store rows data in List of string 
            IList<string> rowsValuesToUpsert = new List<string>();

            var uniqueUpdatedPrimaryKeys = new HashSet<string>();

            // If there are duplicate primary keys, we'll need to pick the LAST (most recent) row per primary key.
            foreach (T row in rows.Reverse())
            {
                if (typeof(T) != typeof(JObject))
                {
                    if (table.HasIdentityColumnPrimaryKeys)
                    {
                        // If the table has an identity column as a primary key then
                        // all rows are guaranteed to be unique so we can insert them all
                        rowsValuesToUpsert.Add(GetColValuesForUpsert(row, table, columnNamesFromItem, parameters));
                    }
                    else
                    {
                        // MySQL Server allows 900 bytes per primary key, so use that as a baseline
                        var combinedPrimaryKey = new StringBuilder(900 * table.PrimaryKeyProperties.Count());
                        // Look up primary key of T. Because we're going in the same order of properties every time,
                        // we can assume that if two rows with the same primary key are in the list, they will collide
                        foreach (PropertyInfo primaryKeyProperty in table.PrimaryKeyProperties)
                        {
                            object value = primaryKeyProperty.GetValue(row);
                            // Identity columns are allowed to be optional, so just skip the key if it doesn't exist
                            if (value == null)
                            {
                                continue;
                            }
                            combinedPrimaryKey.Append(value.ToString());
                        }
                        string combinedPrimaryKeyStr = combinedPrimaryKey.ToString();
                        // If we have already seen this unique primary key, skip this update
                        // If the combined key is empty that means
                        if (uniqueUpdatedPrimaryKeys.Add(combinedPrimaryKeyStr))
                        {
                            //add a column values of a single row
                            rowsValuesToUpsert.Add(GetColValuesForUpsert(row, table, columnNamesFromItem, parameters));
                        }
                    }
                }
                else
                {
                    // ToDo: add check for duplicate primary keys once we find a way to get primary keys.
                    //add column values of a single row
                    rowsValuesToUpsert.Add(GetColValuesForUpsert(row, table, columnNamesFromItem, parameters));

                }
            }

            // concat\join different rows data by comma separate
            newDataQuery = string.Join(", ", rowsValuesToUpsert);
        }

        public class TableInformation
        {
            public List<PrimaryKey> PrimaryKeys { get; }

            public IEnumerable<PropertyInfo> PrimaryKeyProperties { get; }

            /// <summary>
            /// All of the columns, along with their data types, for MySQL to use to turn JSON into a table
            /// </summary>
            public IDictionary<string, string> Columns { get; }

            /// <summary>
            /// List of strings containing each column and its type. ex: ["Cost int", "LastChangeDate datetime(7)"]
            /// </summary>
            public IEnumerable<string> ColumnDefinitions => this.Columns.Select(c => $"{c.Key} {c.Value}");

            /// <summary>
            /// Whether at least one of the primary keys on this table is an identity column
            /// </summary>
            public bool HasIdentityColumnPrimaryKeys { get; }
            /// <summary>
            /// Settings to use when serializing the POCO into MySQL.
            /// Only serialize properties and fields that correspond to MySQL columns.
            /// </summary>
            public JsonSerializerSettings JsonSerializerSettings { get; }

            public TableInformation(List<PrimaryKey> primaryKeys, IEnumerable<PropertyInfo> primaryKeyProperties, IDictionary<string, string> columns, bool hasIdentityColumnPrimaryKeys)
            {
                this.PrimaryKeys = primaryKeys;
                this.PrimaryKeyProperties = primaryKeyProperties;
                this.Columns = columns;
                this.HasIdentityColumnPrimaryKeys = hasIdentityColumnPrimaryKeys;

                // Convert datetime strings to ISO 8061 format to avoid potential errors on the server when converting into a datetime. This
                // is the only format that are an international standard.
                this.JsonSerializerSettings = new JsonSerializerSettings
                {
                    ContractResolver = new DynamicPOCOContractResolver(columns),
                    DateFormatString = ISO_8061_DATETIME_FORMAT
                };
            }

            /// <summary>
            /// Generates MySQL query that can be used to retrieve the Primary Keys of a table
            /// </summary>
            public static string GetPrimaryKeysQuery(MySqlObject table)
            {
                return $@"
                    SELECT
                        cu.{ColumnName},
                        case
                            when isc.EXTRA = 'auto_increment' then 'true'
                            else 'false'
                        end as 'is_autoincrement',
                        case
                            when isc.COLUMN_DEFAULT IS NULL then 'false'
                            else 'true'
                        end as {HasDefault}
                    FROM
                        INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    INNER JOIN
                        INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu ON cu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND cu.TABLE_NAME = tc.TABLE_NAME AND cu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    INNER JOIN
                        INFORMATION_SCHEMA.COLUMNS isc ON isc.TABLE_SCHEMA = tc.TABLE_SCHEMA AND isc.TABLE_NAME = tc.TABLE_NAME AND isc.COLUMN_NAME = cu.COLUMN_NAME
                    WHERE
                        tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    and
                        tc.TABLE_NAME = {table.SingleQuotedName}
                    and
                        tc.TABLE_SCHEMA = {table.SingleQuotedSchema}";
            }

            /// <summary>
            /// Generates MySQL query that can be used to retrieve column names and types of a table
            /// </summary>
            public static string GetColumnDefinitionsQuery(MySqlObject table)
            {
                return $@"
                    select
	                    {ColumnName}, CONCAT(DATA_TYPE, 
		                    case
			                    when CHARACTER_MAXIMUM_LENGTH is not null then CONCAT('(',CHARACTER_MAXIMUM_LENGTH,')')
			                    else ''
		                    end) as {ColumnDefinition}
                    from
	                    INFORMATION_SCHEMA.COLUMNS c
                    where
	                    c.TABLE_NAME = {table.SingleQuotedName}
                    and
                        c.TABLE_SCHEMA = {table.SingleQuotedSchema}";
            }

            public static string GetInsertQuery(MySqlObject table, IEnumerable<string> columnNamesFromItem)
            {
                return $"INSERT INTO {table.AcuteQuotedFullName} ({string.Join(",", columnNamesFromItem)}) VALUES";
            }

            public static string GetOnDuplicateUpdateQuery(IEnumerable<string> columnNamesFromItem)
            {
                // to store rows data in List of string 
                IList<string> formattedUpdateValues = new List<string>();
                foreach (string colName in columnNamesFromItem)
                {
                    string tmpStr = $"{colName} = VALUES({colName})";
                    formattedUpdateValues.Add(tmpStr);
                }

                return $"ON DUPLICATE KEY UPDATE {string.Join(", ", formattedUpdateValues)}";
            }


            /// <summary>
            /// Retrieve (relatively) static information of MySQL Table like primary keys, column names, etc.
            /// in order to generate the MERGE portion of the upsert query.
            /// This only needs to be generated once and can be reused for subsequent upserts.
            /// </summary>
            /// <param name="mysqlConnection">An open connection with which to query MySQL against</param>
            /// <param name="fullName">Full name of table, including schema (if exists).</param>
            /// <param name="logger">ILogger used to log any errors or warnings.</param>
            /// <param name="objectColumnNames">Column names from the object</param>
            /// <returns>TableInformation object containing primary keys, column types, etc.</returns>
            public static TableInformation RetrieveTableInformation(MySqlConnection mysqlConnection, string fullName, ILogger logger, IEnumerable<string> objectColumnNames)
            {
                var table = new MySqlObject(fullName);

                var tableInfoSw = Stopwatch.StartNew();

                // Get all column names and types
                var columnDefinitionsFromMySQL = new Dictionary<string, string>();
                var columnDefinitionsSw = Stopwatch.StartNew();
                try
                {
                    string getColumnDefinitionsQuery = GetColumnDefinitionsQuery(table);
                    var cmdColDef = new MySqlCommand(getColumnDefinitionsQuery, mysqlConnection);
                    using (MySqlDataReader rdr = cmdColDef.ExecuteReaderWithLogging(logger))
                    {
                        while (rdr.Read())
                        {
                            string columnName = rdr[ColumnName].ToString();
                            columnDefinitionsFromMySQL.Add(columnName, rdr[ColumnDefinition].ToString());
                        }
                        columnDefinitionsSw.Stop();
                        logger.LogInformation($"Time taken (ms) to get column definitions: {columnDefinitionsSw.ElapsedMilliseconds}");
                    }

                }
                catch (Exception ex)
                {
                    logger.LogError($"Exception encountered during GetColumnDefinitions. Message:{ex.Message}");
                    // Throw a custom error so that it's easier to decipher.
                    string message = $"Encountered exception while retrieving column names and types for the given table. Cannot generate upsert command without them.";
                    throw new InvalidOperationException(message, ex);
                }

                if (columnDefinitionsFromMySQL.Count == 0)
                {
                    string message = $"Specified table does not exist.";
                    var ex = new InvalidOperationException(message);
                    logger.LogError($"Column Definition does not exist for the given table");
                    throw ex;
                }

                // Query MySQL for table Primary Keys
                var primaryKeys = new List<PrimaryKey>();
                var primaryKeysSw = Stopwatch.StartNew();
                try
                {
                    string getPrimaryKeysQuery = GetPrimaryKeysQuery(table);
                    var cmd = new MySqlCommand(getPrimaryKeysQuery, mysqlConnection);
                    using (MySqlDataReader rdr = cmd.ExecuteReaderWithLogging(logger))
                    {
                        while (rdr.Read())
                        {
                            string columnName = rdr[ColumnName].ToString();
                            primaryKeys.Add(new PrimaryKey(columnName, bool.Parse(rdr[IsAutoIncrement].ToString()), bool.Parse(rdr[HasDefault].ToString())));
                        }
                        primaryKeysSw.Stop();
                        logger.LogInformation($"Time taken (ms) to get PrimaryKeys for the specified table: {primaryKeysSw.ElapsedMilliseconds}");
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError($"Exception encountered while fetching the primary keys for the specified table. Message: {ex.Message}");
                    // Throw a custom error so that it's easier to decipher.
                    string message = $"Encountered exception while retrieving primary keys for the specified table. Cannot generate upsert command without them.";
                    throw new InvalidOperationException(message, ex);
                }

                if (!primaryKeys.Any())
                {
                    string message = $"Did not retrieve any primary keys for the specified table. Cannot generate upsert command without them.";
                    var ex = new InvalidOperationException(message);
                    logger.LogError($"Unable to get primary keys for the specified table");
                    throw ex;
                }

                // Match MySQL Primary Key column names to POCO property objects. Ensure none are missing.
                IEnumerable<PropertyInfo> primaryKeyProperties = typeof(T).GetProperties().Where(f => primaryKeys.Any(k => string.Equals(k.Name, f.Name, StringComparison.Ordinal)));
                IEnumerable<string> primaryKeysFromObject = objectColumnNames.Where(f => primaryKeys.Any(k => string.Equals(k.Name, MySqlObject.GetMySqlColNameFormatted(f), StringComparison.Ordinal)));
                IEnumerable<PrimaryKey> missingPrimaryKeysFromItem = primaryKeys.Where(pk => !objectColumnNames.Any(k => string.Equals(pk.Name, MySqlObject.GetMySqlColNameFormatted(k), StringComparison.Ordinal)));

                bool hasIdentityColumnPrimaryKeys = primaryKeys.Any(k => k.IsAutoIncrement);
                bool hasDefaultColumnPrimaryKeys = primaryKeys.Any(k => k.HasDefault);
                // If none of the primary keys are an identity column or have a default value then we require that all primary keys be present in the POCO so we can
                // generate the MERGE statement correctly
                if (!hasIdentityColumnPrimaryKeys && !hasDefaultColumnPrimaryKeys && missingPrimaryKeysFromItem.Any())
                {
                    string message = $"All primary keys for the specified table need to be found in '{typeof(T)}'";
                    var ex = new InvalidOperationException(message);
                    logger.LogError($"Missing Primary Keys for the specified table");
                    throw ex;
                }

                tableInfoSw.Stop();
                logger.LogInformation($"Time taken(ms) to get Table information: {tableInfoSw.ElapsedMilliseconds}");
                return new TableInformation(primaryKeys, primaryKeyProperties, columnDefinitionsFromMySQL, hasIdentityColumnPrimaryKeys);
            }
        }

        public class DynamicPOCOContractResolver : DefaultContractResolver
        {
            private readonly IDictionary<string, string> _propertiesToSerialize;

            public DynamicPOCOContractResolver(IDictionary<string, string> sqlColumns)
            {
                // we only want to serialize POCO properties that correspond to MySQL columns
                this._propertiesToSerialize = sqlColumns;
            }

            protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
            {
                var properties = base
                    .CreateProperties(type, memberSerialization)
                    .ToDictionary(p => p.PropertyName);

                // Make sure the ordering of columns matches that of MySQL
                // Necessary for proper matching of column names to JSON that is generated for each batch of data
                IList<JsonProperty> propertiesToSerialize = new List<JsonProperty>(properties.Count);
                foreach (KeyValuePair<string, string> column in this._propertiesToSerialize)
                {
                    if (properties.TryGetValue(column.Key, out JsonProperty value))
                    {
                        JsonProperty sqlColumn = value;
                        sqlColumn.PropertyName = sqlColumn.PropertyName;
                        propertiesToSerialize.Add(sqlColumn);
                    }
                }

                return propertiesToSerialize;
            }
        }

        public void Dispose()
        {
            this._rowLock.Dispose();
        }
    }
}