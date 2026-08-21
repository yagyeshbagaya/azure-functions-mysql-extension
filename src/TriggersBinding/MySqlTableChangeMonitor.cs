// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using static Microsoft.Azure.WebJobs.Extensions.MySql.MySqlTriggerConstants;

namespace Microsoft.Azure.WebJobs.Extensions.MySql
{
    /// <summary>
    /// Watches for changes in the user table, invokes user function if changes are found.
    /// </summary>
    /// <typeparam name="T">POCO class representing the row in the user table</typeparam>
    internal class MySqlTableChangeMonitor<T> : IDisposable
    {
        #region Constants
        /// <remarks>
        /// Leases are held for approximately (LeaseRenewalIntervalInSeconds * MaxLeaseRenewalCount) seconds. It is
        /// required to have at least one of (LeaseIntervalInSeconds / LeaseRenewalIntervalInSeconds) attempts to
        /// renew the lease succeed to prevent it from expiring.
        /// </remarks>
        private const int MaxLeaseRenewalCount = 10;
        public const int LeaseIntervalInSeconds = 60;
        private const int LeaseRenewalIntervalInSeconds = 15;
        private const int MaxRetryReleaseLeases = 3;


        #endregion Constants

        private readonly string _connectionString;
        private readonly string _userTableId;
        private readonly MySqlObject _userTable;
        private readonly string _userFunctionId;
        private readonly string _leasesTableName;
        private readonly IReadOnlyList<(string name, string type)> _primaryKeyColumns;
        private readonly IReadOnlyList<string> _userTableColumns;
        private readonly ITriggeredFunctionExecutor _executor;
        private readonly MySqlOptions _mysqlOptions;
        private readonly ILogger _logger;

        // a list of primary key colum's Names
        private readonly IEnumerable<string> _primaryKeyColumnNames;
        /// <summary>
        /// Maximum number of changes to process in each iteration of the loop
        /// </summary>
        private readonly int _maxBatchSize;
        /// <summary>
        /// Delay in ms between processing each batch of changes
        /// </summary>
        private readonly int _pollingIntervalInMs;

        private readonly CancellationTokenSource _cancellationTokenSourceCheckForChanges = new CancellationTokenSource();
        private readonly CancellationTokenSource _cancellationTokenSourceRenewLeases = new CancellationTokenSource();
        private readonly CancellationTokenSource _cancellationTokenSourceExecutor = new CancellationTokenSource();

        // The semaphore gets used by lease-renewal loop to ensure that '_state' stays set to 'ProcessingChanges' while
        // the leases are being renewed. The change-consumption loop requires to wait for the semaphore before modifying
        // the value of '_state' back to 'CheckingForChanges'. Since the field '_rows' is only updated if the value of
        // '_state' is set to 'CheckingForChanges', this guarantees that '_rows' will stay same while it is being
        // iterated over inside the lease-renewal loop.
        private readonly SemaphoreSlim _rowsLock = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Rows that are currently being processed
        /// </summary>
        private IReadOnlyList<IReadOnlyDictionary<string, object>> _rowsToProcess = new List<IReadOnlyDictionary<string, object>>();
        /// <summary>
        /// Rows that have been processed and now need to have their leases released
        /// </summary>
        private IReadOnlyList<IReadOnlyDictionary<string, object>> _rowsToRelease = new List<IReadOnlyDictionary<string, object>>();
        private int _leaseRenewalCount = 0;
        private State _state = State.CheckingForChanges;

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlTableChangeMonitor{T}" />> class.
        /// </summary>
        /// <param name="connectionString">MySQL connection string used to connect to user database</param>
        /// <param name="userTableId">MySQL object ID of the user table</param>
        /// <param name="userTable"><see cref="MySqlObject" /> instance created with user table name</param>
        /// <param name="userFunctionId">Unique identifier for the user function</param>
        /// <param name="leasesTableName">Name of the leases table</param>
        /// <param name="primaryKeyColumns">List of primary key column names in the user table</param>
        /// <param name="userTableColumns">List of all column names in the user table</param>
        /// <param name="executor">Defines contract for triggering user function</param>
        /// <param name="mysqlOptions"></param>
        /// <param name="logger">Facilitates logging of messages</param>
        /// <param name="configuration">Provides configuration values</param>
        public MySqlTableChangeMonitor(
            string connectionString,
            string userTableId,
            MySqlObject userTable,
            string userFunctionId,
            string leasesTableName,
            IReadOnlyList<(string name, string type)> primaryKeyColumns,
            IReadOnlyList<string> userTableColumns,
            ITriggeredFunctionExecutor executor,
            MySqlOptions mysqlOptions,
            ILogger logger,
            IConfiguration configuration)
        {
            this._connectionString = !string.IsNullOrEmpty(connectionString) ? connectionString : throw new ArgumentNullException(nameof(connectionString));
            this._userTableId = userTableId;
            this._userTable = userTable ?? throw new ArgumentNullException(nameof(userTable));
            this._userFunctionId = userFunctionId ?? throw new ArgumentNullException(nameof(userFunctionId));
            this._leasesTableName = leasesTableName ?? throw new ArgumentNullException(nameof(leasesTableName));
            this._primaryKeyColumns = primaryKeyColumns ?? throw new ArgumentNullException(nameof(primaryKeyColumns));
            this._userTableColumns = userTableColumns ?? throw new ArgumentNullException(nameof(userTableColumns));
            this._mysqlOptions = mysqlOptions ?? throw new ArgumentNullException(nameof(mysqlOptions));
            this._executor = executor ?? throw new ArgumentNullException(nameof(executor));
            this._logger = logger ?? throw new ArgumentNullException(nameof(logger));


            this._primaryKeyColumnNames = this._primaryKeyColumns.Select(col => $"{col.name}");

            int? configuredMaxBatchSize = configuration.GetValue<int?>(ConfigKey_MySqlTrigger_MaxBatchSize) ?? configuration.GetValue<int?>(ConfigKey_MySqlTrigger_BatchSize);
            this._maxBatchSize = configuredMaxBatchSize ?? this._mysqlOptions.MaxBatchSize;
            if (this._maxBatchSize <= 0)
            {
                throw new InvalidOperationException($"Invalid value for configuration setting '{ConfigKey_MySqlTrigger_MaxBatchSize}'. Ensure that the value is a positive integer.");
            }

            int? configuredPollingInterval = configuration.GetValue<int?>(ConfigKey_MySqlTrigger_PollingInterval);
            this._pollingIntervalInMs = configuredPollingInterval ?? this._mysqlOptions.PollingIntervalMs;
            if (this._pollingIntervalInMs <= 0)
            {
                throw new InvalidOperationException($"Invalid value for configuration setting '{ConfigKey_MySqlTrigger_PollingInterval}'. Ensure that the value is a positive integer.");
            }

#pragma warning disable CS4014 // Queue the below tasks and exit. Do not wait for their completion.
            _ = Task.Run(() =>
            {
                this.RunChangeConsumptionLoopAsync();
                this.RunLeaseRenewalLoopAsync();
            });
#pragma warning restore CS4014
        }

        public void Dispose()
        {
            // When the CheckForChanges loop is finished, it will cancel the lease renewal loop.
            this._cancellationTokenSourceCheckForChanges.Cancel();
        }

        /// <summary>
        /// Executed once every <see cref="_pollingIntervalInMs"/> period. Each iteration will go through a series of state
        /// changes, if an error occurs in any of them then it will skip the rest of the states and try again in the next
        /// iteration.
        /// It starts in the <see cref="State.CheckingForChanges"/> check and queries the change/leases tables for changes on the
        /// user's table.
        /// If any are found, the state is transitioned to <see cref="State.ProcessingChanges"/> and the user's
        /// function is executed with the found changes.
        /// Finally the state moves to <see cref="State.Cleanup" /> and if the execution was successful,
        /// the leases on "_rows" are released. Finally the state transitions to <see cref="State.CheckingForChanges"/>
        /// once again and starts over at the next iteration.
        /// </summary>
        private async Task RunChangeConsumptionLoopAsync()
        {
            this._logger.LogDebug($"Starting change consumption loop. MaxBatchSize: {this._maxBatchSize} PollingIntervalMs: {this._pollingIntervalInMs}");

            try
            {
                CancellationToken token = this._cancellationTokenSourceCheckForChanges.Token;

                using (var connection = new MySqlConnection(this._connectionString))
                {
                    await connection.OpenAsync(token);

                    bool forceReconnect = false;
                    // Check for cancellation request only after a cycle of checking and processing of changes completes.
                    while (!token.IsCancellationRequested)
                    {
                        bool isConnected = await connection.TryEnsureConnected(forceReconnect, this._logger, "ChangeConsumptionConnection", token);
                        if (!isConnected)
                        {
                            // If we couldn't reconnect then wait our delay and try again
                            await Task.Delay(TimeSpan.FromMilliseconds(this._pollingIntervalInMs), token);
                            continue;
                        }
                        else
                        {
                            forceReconnect = false;
                        }

                        try
                        {
                            // Process states sequentially since we normally expect the state to transition at the end
                            // of each previous state - but if an unexpected error occurs we'll skip the rest and then
                            // retry that state after the delay
                            if (this._state == State.CheckingForChanges)
                            {
                                await this.GetTableChangesAsync(connection, token);
                            }
                            if (this._state == State.ProcessingChanges)
                            {
                                await this.ProcessTableChangesAsync(token);
                            }
                            if (this._state == State.Cleanup)
                            {
                                await this.ReleaseLeasesAsync(connection, token);
                            }
                        }
                        catch (Exception e) when (connection.IsBrokenOrClosed())        // TODO: e.IsFatalMySqlException() || - check mysql corresponding 
                        {
                            // Retry connection if there was a fatal MySQL exception or something else caused the connection to be closed
                            // since that indicates some other issue occurred (such as dropped network) and may be able to be recovered
                            this._logger.LogError($"Fatal MySQL Client exception processing changes. Will attempt to reestablish connection in {this._pollingIntervalInMs}ms. Exception = {e.Message}");
                            forceReconnect = true;
                        }
                        await Task.Delay(TimeSpan.FromMilliseconds(this._pollingIntervalInMs), token);
                    }
                }
            }
            catch (Exception e)
            {
                // Only want to log the exception if it wasn't caused by StopAsync being called, since Task.Delay
                // throws an exception if it's cancelled.
                if (e.GetType() != typeof(TaskCanceledException))
                {
                    this._logger.LogError($"Exiting change consumption loop due to exception: {e.GetType()}. Exception message: {e.Message}");
                }
                throw;
            }
            finally
            {
                // If this thread exits due to any reason, then the lease renewal thread should exit as well. Otherwise,
                // it will keep looping perpetually.
                this._cancellationTokenSourceCheckForChanges.Dispose();
                this._cancellationTokenSourceExecutor.Dispose();
            }
        }

        /// <summary>
        /// Queries the change/leases tables to check for new changes on the user's table. If any are found, stores the
        /// change along with the corresponding data from the user table in "_rows".
        /// </summary>
        private async Task GetTableChangesAsync(MySqlConnection connection, CancellationToken token)
        {
            try
            {
                var transactionSw = Stopwatch.StartNew();
                long getChangesDurationMs = 0L, acquireLeasesDurationMs = 0L;

                using (MySqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var rows = new List<IReadOnlyDictionary<string, object>>();
                        // query for new changes.
                        using (MySqlCommand getChangesCommand = this.BuildGetChangesCommand(connection, transaction))
                        {
                            var commandSw = Stopwatch.StartNew();
                            this._logger.LogInformation($"Looking for latest changes on the configured table");

                            using (MySqlDataReader reader = getChangesCommand.ExecuteReaderWithLogging(this._logger))
                            {
                                while (reader.Read())
                                {
                                    token.ThrowIfCancellationRequested();
                                    rows.Add(MySqlBindingUtilities.BuildDictionaryFromMySqlRow(reader));
                                }
                            }
                            getChangesDurationMs = commandSw.ElapsedMilliseconds;
                        }

                        // If changes were found
                        if (rows.Count > 0)
                        {
                            this._logger.LogInformation($"The total no of rows found to process is {rows.Count}");

                            using (MySqlCommand acquireLeasesCommand = this.BuildAcquireLeasesCommand(connection, transaction, rows))
                            {
                                var commandSw = Stopwatch.StartNew();
                                this._logger.LogDebug($"Acquiring lease ...");
                                await acquireLeasesCommand.ExecuteNonQueryAsyncWithLogging(this._logger, token);
                                acquireLeasesDurationMs = commandSw.ElapsedMilliseconds;
                            }
                        }

                        transaction.Commit();

                        // Set the rows for processing, now since the leases are acquired.
                        await this._rowsLock.WaitAsync(token);
                        this._rowsToProcess = rows;
                        this._state = State.ProcessingChanges;
                        this._rowsLock.Release();
                    }
                    catch (Exception)
                    {
                        try
                        {
                            transaction.Rollback();
                        }
                        catch (Exception ex)
                        {
                            this._logger.LogError($"Failed to rollback transaction due to exception: {ex.GetType()}. Exception message: {ex.Message}");
                        }
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                // If there's an exception in any part of the process, we want to clear all of our data in memory and
                // retry checking for changes again.
                await this._rowsLock.WaitAsync(token);
                this._rowsToProcess = new List<IReadOnlyDictionary<string, object>>();
                this._rowsLock.Release();

                this._logger.LogError($"Failed to check for changes in the specified table due to exception: {e.GetType()}. Exception message: {e.Message}");
                if (connection.IsBrokenOrClosed())      // TODO: e.IsFatalMySqlException() || - check mysql corresponding
                {
                    // If we get a fatal MySQL Client exception or the connection is broken let it bubble up so we can try to re-establish the connection
                    throw;
                }
            }
        }

        private async Task ProcessTableChangesAsync(CancellationToken token)
        {
            if (this._rowsToProcess.Count > 0)
            {
                IReadOnlyList<MySqlChange<T>> changes = null;

                try
                {
                    changes = this.ProcessChanges();
                }
                catch (Exception e)
                {
                    // Either there's a bug or we're in a bad state so not much we can do here. We'll try clearing
                    //  our state and retry getting the changes from the top again in case something broke while
                    // fetching the changes.
                    // It doesn't make sense to retry processing the changes immediately since this isn't a connection-based issue.
                    // We could probably send up the changes we were able to process and just skip the ones we couldn't, but given
                    // that this is not a case we expect would happen during normal execution we'll err on the side of caution for
                    // now and just retry getting the whole set of changes.
                    this._logger.LogError($"Failed to compose trigger parameter value for the specified table due to exception: {e.GetType()}. Exception message: {e.Message}");
                    await this.ClearRowsAsync(token);
                }

                if (changes != null)
                {
                    var input = new TriggeredFunctionData() { TriggerValue = changes };

                    // var stopwatch = Stopwatch.StartNew();

                    FunctionResult result = await this._executor.TryExecuteAsync(input, this._cancellationTokenSourceExecutor.Token);
                    // long durationMs = stopwatch.ElapsedMilliseconds;
                    if (result.Succeeded)
                    {
                        this._logger.LogInformation("Function Trigger executed successfully.");
                        await this._rowsLock.WaitAsync(token);
                        this._rowsToRelease = this._rowsToProcess;
                        this._rowsToProcess = new List<IReadOnlyDictionary<string, object>>();
                        this._rowsLock.Release();
                    }
                    else
                    {
                        this._logger.LogError($"Exception encountered while executing the Function Trigger. Exception: {result.Exception}");
                    }
                    this._state = State.Cleanup;
                }
            }
            else
            {
                // This ideally should never happen, but as a safety measure ensure that if we tried to process changes but there weren't
                // any we still ensure everything is reset to a clean state
                await this.ClearRowsAsync(token);
            }
        }

        /// <summary>
        /// Executed once every <see cref="LeaseRenewalIntervalInSeconds"/> seconds. If the state of the change monitor is
        /// <see cref="State.ProcessingChanges"/>, then we will renew the leases held by the change monitor on "_rows".
        /// </summary>
        private async void RunLeaseRenewalLoopAsync()
        {
            this._logger.LogDebug("Starting lease renewal loop.");

            try
            {
                CancellationToken token = this._cancellationTokenSourceRenewLeases.Token;

                using (var connection = new MySqlConnection(this._connectionString))
                {
                    await connection.OpenAsync(token);

                    bool forceReconnect = false;
                    while (!token.IsCancellationRequested)
                    {
                        bool isConnected = await connection.TryEnsureConnected(forceReconnect, this._logger, "LeaseRenewalLoopConnection", token);
                        if (!isConnected)
                        {
                            // If we couldn't reconnect then wait our delay and try again
                            await Task.Delay(TimeSpan.FromSeconds(LeaseRenewalIntervalInSeconds), token);
                            continue;
                        }
                        else
                        {
                            forceReconnect = false;
                        }
                        try
                        {
                            await this.RenewLeasesAsync(connection, token);
                        }
                        catch (Exception e) when (e.IsDeadlockException() || /*e.IsFatalSqlException() || */connection.IsBrokenOrClosed())
                        {
                            // Retry connection if there was a fatal MySQL exception or something else caused the connection to be closed
                            // since that indicates some other issue occurred (such as dropped network) and may be able to be recovered
                            forceReconnect = true;
                        }

                        await Task.Delay(TimeSpan.FromSeconds(LeaseRenewalIntervalInSeconds), token);
                    }
                }
            }
            catch (Exception e)
            {
                // Only want to log the exception if it wasn't caused by StopAsync being called, since Task.Delay throws
                // an exception if it's cancelled.
                if (e.GetType() != typeof(TaskCanceledException))
                {
                    this._logger.LogError($"Exiting lease renewal loop due to exception: {e.GetType()}. Exception message: {e.Message}");
                }
            }
            finally
            {
                this._cancellationTokenSourceRenewLeases.Dispose();
            }
        }

        private async Task RenewLeasesAsync(MySqlConnection connection, CancellationToken token)
        {
            await this._rowsLock.WaitAsync(token);

            if (this._state == State.ProcessingChanges && this._rowsToProcess.Count > 0)
            {
                // Use a transaction to automatically release the app lock when we're done executing the query
                using (MySqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (MySqlCommand renewLeasesCommand = this.BuildRenewLeasesCommand(connection, transaction))
                        {
                            var stopwatch = Stopwatch.StartNew();
                            this._logger.LogDebug($"Renewing lease ...");
                            int rowsAffected = await renewLeasesCommand.ExecuteNonQueryAsyncWithLogging(this._logger, token);
                            long durationMs = stopwatch.ElapsedMilliseconds;

                            if (rowsAffected > 0)
                            {
                                // Only send an event if we actually updated rows to reduce the overall number of events we send
                                this._logger.LogInformation($"Updated the Leases table");
                            }
                            transaction.Commit();
                            this._logger.LogDebug($"The lease expiration time renewed by {LeaseRenewalIntervalInSeconds} seconds, for the rows(under process) in the lease table");
                        }
                    }
                    catch (Exception e)
                    {
                        // This catch block is necessary so that the finally block is executed even in the case of an exception
                        // (see https://docs.microsoft.com/dotnet/csharp/language-reference/keywords/try-finally, third
                        // paragraph). If we fail to renew the leases, multiple workers could be processing the same change
                        // data, but we have functionality in place to deal with this (see design doc).
                        this._logger.LogError($"Failed to renew leases due to exception: {e.GetType()}. Exception message: {e.Message}");

                        try
                        {
                            transaction.Rollback();
                        }
                        catch (Exception e2)
                        {
                            this._logger.LogError($"RenewLeases - Failed to rollback transaction due to exception: {e2.GetType()}. Exception message: {e2.Message}");
                        }
                    }
                    finally
                    {
                        // Do we want to update this count even in the case of a failure to renew the leases? Probably,
                        // because the count is simply meant to indicate how much time the other thread has spent processing
                        // changes essentially.
                        this._leaseRenewalCount += 1;

                        // If this thread has been cancelled, then the _cancellationTokenSourceExecutor could have already
                        // been disposed so shouldn't cancel it.
                        if (this._leaseRenewalCount == MaxLeaseRenewalCount && !token.IsCancellationRequested)
                        {
                            this._logger.LogWarning("Call to execute the function (TryExecuteAsync) seems to be stuck, so it is being cancelled");

                            // If we keep renewing the leases, the thread responsible for processing the changes is stuck.
                            // If it's stuck, it has to be stuck in the function execution call (I think), so we should
                            // cancel the call.
                            this._cancellationTokenSourceExecutor.Cancel();
                            //this._cancellationTokenSourceExecutor = new CancellationTokenSource();
                        }
                    }
                }
            }

            // Want to always release the lock at the end, even if renewing the leases failed.
            this._rowsLock.Release();
        }

        /// <summary>
        /// Resets the in-memory state of the change monitor and sets it to start polling for changes again.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        private async Task ClearRowsAsync(CancellationToken token)
        {
            await this._rowsLock.WaitAsync(token);
            this._leaseRenewalCount = 0;
            this._state = State.CheckingForChanges;
            if (this._rowsToProcess.Count > 0)
            {
                this._logger.LogDebug($"Clearing internal state for {this._rowsToProcess.Count} rows");
            }
            this._rowsToProcess = new List<IReadOnlyDictionary<string, object>>();
            this._rowsLock.Release();
        }

        /// <summary>
        /// Computes the Last updated polled time
        /// </summary>
        private string RecomputeLastPolledTime()
        {
            string maxTimeStamp = "";

            foreach (IReadOnlyDictionary<string, object> row in this._rowsToRelease)
            {
                string strTimeStamp = row[UpdateAtColumnName].ToString();

                if (string.Compare(strTimeStamp, maxTimeStamp, StringComparison.Ordinal) > 0)
                {
                    maxTimeStamp = strTimeStamp;
                }
            }

            return maxTimeStamp;
        }

        /// <summary>
        /// Releases the leases held on "_rowsToRelease" and updates the state tables with the latest sync version we've processed up to.
        /// </summary>
        /// <returns></returns>
        private async Task ReleaseLeasesAsync(MySqlConnection connection, CancellationToken token)
        {
            if (this._rowsToRelease.Count > 0)
            {
                string newLastPolledTime = this.RecomputeLastPolledTime();
                bool retrySucceeded = false;
                long unprocessedChangesCount = 0;

                for (int retryCount = 1; retryCount <= MaxRetryReleaseLeases && !retrySucceeded; retryCount++)
                {
                    using (MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.RepeatableRead))
                    {
                        try
                        {
                            // Release the leases held on "_rowsToRelease".
                            using (MySqlCommand releaseLeasesCommand = this.BuildReleaseLeasesCommand(connection, transaction))
                            {
                                var commandSw = Stopwatch.StartNew();
                                this._logger.LogDebug($"Releasing lease ...");
                                int rowsUpdated = await releaseLeasesCommand.ExecuteNonQueryAsyncWithLogging(this._logger, token);
                                long releaseLeasesDurationMs = commandSw.ElapsedMilliseconds;
                            }

                            // count unprocessed changes where update is done before 'newLastPolledTime'.
                            using (MySqlCommand countUnprocessedChangesCommand = this.BuildCountUnprocessedChanges(connection, transaction, newLastPolledTime))
                            {
                                var commandSw = Stopwatch.StartNew();
                                unprocessedChangesCount = (long)await countUnprocessedChangesCommand.ExecuteScalarAsyncWithLogging(this._logger, token);
                                this._logger.LogDebug($"Unprocessed count is {unprocessedChangesCount} before {newLastPolledTime}");
                            }

                            if (unprocessedChangesCount == 0)
                            {
                                using (MySqlCommand updateLastPollingTimeCommand = this.BuildUpdateGlobalStateTableLastPollingTime(connection, transaction, newLastPolledTime))
                                {
                                    int rowsUpdated = await updateLastPollingTimeCommand.ExecuteNonQueryAsyncWithLogging(this._logger, token);
                                    this._logger.LogDebug($"Updated {GlobalStateTableLastPolledTimeColumnName} to " + newLastPolledTime);
                                }

                                using (MySqlCommand deleteProcessedChangesCommand = this.BuildDeleteProcessedChangesInLeaseTable(connection, transaction))
                                {
                                    int rowsUpdated = await deleteProcessedChangesCommand.ExecuteNonQueryAsyncWithLogging(this._logger, token);
                                    this._logger.LogDebug($"Total {rowsUpdated} rows cleaned from the Lease Table");
                                }
                            }

                            transaction.Commit();

                            retrySucceeded = true;
                            this._rowsToRelease = new List<IReadOnlyDictionary<string, object>>();
                        }
                        catch (Exception ex)
                        {
                            if (retryCount < MaxRetryReleaseLeases)
                            {
                                this._logger.LogError($"Failed to execute MySQL commands to release leases in attempt: {retryCount} for the specified table due to exception: {ex.GetType()}. Exception message: {ex.Message}");
                            }
                            else
                            {
                                this._logger.LogError($"Failed to release leases for the specified table after {MaxRetryReleaseLeases} attempts due to exception: {ex.GetType()}. Exception message: {ex.Message}");
                            }

                            try
                            {
                                transaction.Rollback();
                            }
                            catch (Exception ex2)
                            {
                                this._logger.LogError($"Failed to rollback transaction due to exception: {ex2.GetType()}. Exception message: {ex2.Message}");
                            }
                        }
                    }
                }
            }
            await this.ClearRowsAsync(token);
        }

        /// <summary>
        /// Builds up the list of <see cref="MySqlChange{T}"/> passed to the user's triggered function based on the data
        /// stored in "_rows". If any of the changes correspond to a deleted row, then the <see cref="MySqlChange{T}.Item" />
        /// will be populated with only the primary key values of the deleted row.
        /// </summary>
        /// <returns>The list of changes</returns>
        private IReadOnlyList<MySqlChange<T>> ProcessChanges()
        {
            var changes = new List<MySqlChange<T>>();
            foreach (IReadOnlyDictionary<string, object> row in this._rowsToProcess)
            {
                //MySqlChangeOperation operation = GetChangeOperation(row);
                MySqlChangeOperation operation = MySqlChangeOperation.Update;

                var item = row.Select(dict => dict).ToDictionary(pair => pair.Key, pair => pair.Value);

                changes.Add(new MySqlChange<T>(operation, Utils.JsonDeserializeObject<T>(Utils.JsonSerializeObject(item))));
            }
            return changes;
        }

        /// <summary>
        /// Builds the query to check for changes on the user's table (<see cref="RunChangeConsumptionLoopAsync()"/>).
        /// </summary>
        /// <param name="connection">The connection to add to the returned MySqlCommand</param>
        /// <param name="transaction">The transaction to add to the returned MySqlCommand</param>
        /// <returns>The MySqlCommand populated with the query and appropriate parameters</returns>
        private MySqlCommand BuildGetChangesCommand(MySqlConnection connection, MySqlTransaction transaction)
        {
            string selectList = string.Join(", ", this._userTableColumns.Select(col => $"u.{col.AsAcuteQuotedString()}"));
            string leasesTableJoinCondition = string.Join(" AND ", this._primaryKeyColumns.Select(col => $"u.{col.name.AsAcuteQuotedString()} = l.{col.name.AsAcuteQuotedString()}"));

            string primaryKeysAsc = string.Join("ASC , ", this._primaryKeyColumnNames);

            string getChangesQuery = $@"
                        SELECT {selectList}, 
                        DATE_FORMAT(u.{UpdateAtColumnName}, '%Y-%m-%d %H:%i:%s') AS {UpdateAtColumnName},
                        l.{LeasesTableAttemptCountColumnName},
                        l.{LeasesTableLeaseExpirationTimeColumnName}
                        FROM {this._userTable.AcuteQuotedFullName} AS u
                        LEFT JOIN {this._leasesTableName} AS l ON {leasesTableJoinCondition}
                        WHERE 
                            ({UpdateAtColumnName} > (select {GlobalStateTableLastPolledTimeColumnName} from {GlobalStateTableName} where {GlobalStateTableUserFunctionIDColumnName} = '{this._userFunctionId}' AND {GlobalStateTableUserTableIDColumnName} = '{this._userTableId}'))
                            AND
                            (   (l.{LeasesTableLeaseExpirationTimeColumnName} IS NULL) 
                                OR
                                (l.{LeasesTableLeaseExpirationTimeColumnName} < {MYSQL_FUNC_CURRENTTIME})
                            )
                            AND
                            (   (l.{LeasesTableAttemptCountColumnName} IS NULL)
                                OR 
                                (l.{LeasesTableAttemptCountColumnName} < {MaxChangeProcessAttemptCount})
                            )
                            AND
                            (   (l.{LeasesTableSyncCompletedTime} IS NULL)
                                OR 
                                (l.{LeasesTableSyncCompletedTime} < {UpdateAtColumnName})
                            )
                        ORDER BY u.{UpdateAtColumnName} ASC, {primaryKeysAsc}
                        LIMIT {this._maxBatchSize};
                        ";

            return new MySqlCommand(getChangesQuery, connection, transaction);
        }

        /// <summary>
        /// arrange rows data into a values\insert format />).
        /// </summary>
        /// <param name="rows">Dictionary representing the table rows on which leases should be acquired</param>
        /// <returns>The MySqlCommand populated with the query and appropriate parameters</returns>
        private string BuildRowDataInValuesFormat(IReadOnlyList<IReadOnlyDictionary<string, object>> rows)
        {
            //the default values of attemptCount column, default value of LeaseExpirationTime column
            string lastColumnValues = $"{InitialValueAttemptCount}, DATE_ADD({MYSQL_FUNC_CURRENTTIME}, INTERVAL {LeaseIntervalInSeconds} SECOND)";
            IEnumerable<string> rowData = rows.Select(row => $"( {string.Join(", ", row.Where(kvp => this._primaryKeyColumnNames.Contains(kvp.Key)).Select(kp => $"'{Convert.ToString(kp.Value, CultureInfo.InvariantCulture).AsSingleQuoteEscapedString()}'"))}, {lastColumnValues} )");
            string rowDataCombined = string.Join(", ", rowData);

            return rowDataCombined;
        }

        /// <summary>
        /// Builds the query to acquire leases on the rows in "_rows" if changes are detected in the user's table
        /// (<see cref="RunChangeConsumptionLoopAsync()"/>).
        /// </summary>
        /// <param name="connection">The connection to add to the returned MySqlCommand</param>
        /// <param name="transaction">The transaction to add to the returned MySqlCommand</param>
        /// <param name="rows">Dictionary representing the table rows on which leases should be acquired</param>
        /// <returns>The MySqlCommand populated with the query and appropriate parameters</returns>
        private MySqlCommand BuildAcquireLeasesCommand(MySqlConnection connection, MySqlTransaction transaction, IReadOnlyList<IReadOnlyDictionary<string, object>> rows)
        {
            string primaryKeys = string.Join(", ", this._primaryKeyColumnNames);
            string rowDataFormatted = this.BuildRowDataInValuesFormat(rows);

            string acquireLeasesQuery = $@" INSERT INTO {this._leasesTableName}
                                            ({primaryKeys}, {LeasesTableAttemptCountColumnName}, {LeasesTableLeaseExpirationTimeColumnName})
                                        values
                                            {rowDataFormatted}
                                        ON DUPLICATE KEY UPDATE
                                            {LeasesTableAttemptCountColumnName} = {LeasesTableAttemptCountColumnName} + 1,
                                            {LeasesTableLeaseExpirationTimeColumnName} = DATE_ADD({MYSQL_FUNC_CURRENTTIME}, INTERVAL {LeaseIntervalInSeconds} SECOND)
                                        ;";

            var acquireLeasesCommand = new MySqlCommand(acquireLeasesQuery, connection, transaction);
            return acquireLeasesCommand;
        }

        /// <summary>
        /// Builds the query to renew leases on the rows in "_rows" (<see cref="RenewLeasesAsync(MySqlConnection,CancellationToken)"/>).
        /// </summary>
        /// <param name="connection">The connection to add to the returned SqlCommand</param>
        /// <param name="transaction">The transaction to add to the returned SqlCommand</param>
        /// <returns>The SqlCommand populated with the query and appropriate parameters</returns>
        private MySqlCommand BuildRenewLeasesCommand(MySqlConnection connection, MySqlTransaction transaction)
        {
            IEnumerable<string> listMatchCondition = this._rowsToProcess.Select(row => $"( {string.Join(" AND ", row.Where(kvp => this._primaryKeyColumnNames.Contains(kvp.Key)).Select(kp => $"{kp.Key} = '{Convert.ToString(kp.Value, CultureInfo.InvariantCulture).AsSingleQuoteEscapedString()}'"))} )");

            string combinedMatchConditions = string.Join(" OR ", listMatchCondition);

            string renewLeasesQuery = $@"UPDATE {this._leasesTableName}
                                        SET
                                            {LeasesTableLeaseExpirationTimeColumnName} = DATE_ADD({LeasesTableLeaseExpirationTimeColumnName}, INTERVAL {LeaseRenewalIntervalInSeconds} SECOND)
                                        WHERE
                                            {combinedMatchConditions}
                                        ;";

            var renewLeasesCommand = new MySqlCommand(renewLeasesQuery, connection, transaction);
            return renewLeasesCommand;
        }

        /// <summary>
        /// Builds the query to release leases on the rows in "_rowsToRelease" after successful invocation of the user's function
        /// (<see cref="RunChangeConsumptionLoopAsync()"/>).
        /// </summary>
        /// <param name="connection">The connection to add to the returned SqlCommand</param>
        /// <param name="transaction">The transaction to add to the returned SqlCommand</param>
        /// <returns>The SqlCommand populated with the query and appropriate parameters</returns>
        private MySqlCommand BuildReleaseLeasesCommand(MySqlConnection connection, MySqlTransaction transaction)
        {
            IEnumerable<string> listMatchCondition = this._rowsToRelease.Select(row => $"( {string.Join(" AND ", row.Where(kvp => this._primaryKeyColumnNames.Contains(kvp.Key)).Select(kp => $"{kp.Key} = '{Convert.ToString(kp.Value, CultureInfo.InvariantCulture).AsSingleQuoteEscapedString()}'"))} )");

            string combinedMatchConditions = string.Join(" OR ", listMatchCondition);

            string releaseLeasesQuery = $@"UPDATE {this._leasesTableName}
                                        SET
                                            {LeasesTableAttemptCountColumnName} = 0,
                                            {LeasesTableLeaseExpirationTimeColumnName} = NULL,
                                            {LeasesTableSyncCompletedTime} = {MYSQL_FUNC_CURRENTTIME}
                                        WHERE
                                            {combinedMatchConditions}
                                        ;";

            var releaseLeasesCommand = new MySqlCommand(releaseLeasesQuery, connection, transaction);
            return releaseLeasesCommand;
        }

        /// <summary>
        /// Builds the query to get count of unprocessed changes in user's table (<see cref="RunChangeConsumptionLoopAsync()"/>).
        /// </summary>
        /// <param name="connection">The connection to add to the returned MySqlCommand</param>
        /// <param name="transaction">The transaction to add to the returned MySqlCommand</param>
        /// <param name="newLastPolledTime">The last polled time to be updated</param>
        /// <returns>The MySqlCommand populated with the query and appropriate parameters</returns>
        private MySqlCommand BuildCountUnprocessedChanges(MySqlConnection connection, MySqlTransaction transaction, string newLastPolledTime)
        {
            string leasesTableJoinCondition = string.Join(" AND ", this._primaryKeyColumns.Select(col => $"u.{col.name.AsAcuteQuotedString()} = l.{col.name.AsAcuteQuotedString()}"));

            string countUnprocessedQuery = $@"
                        SELECT COUNT(*) 
                        FROM {this._userTable.AcuteQuotedFullName} AS u
                        LEFT JOIN {this._leasesTableName} AS l ON {leasesTableJoinCondition}
                        WHERE 
                            ({UpdateAtColumnName} > (select {GlobalStateTableLastPolledTimeColumnName} from {GlobalStateTableName} where {GlobalStateTableUserFunctionIDColumnName} = '{this._userFunctionId}' AND {GlobalStateTableUserTableIDColumnName} = '{this._userTableId}'))
                            AND
                            ({UpdateAtColumnName} <= TIMESTAMP('{newLastPolledTime}'))
                            AND
                            (   (l.{LeasesTableSyncCompletedTime} IS NULL)
                                OR 
                                (l.{LeasesTableSyncCompletedTime} < {UpdateAtColumnName})
                            )
                            AND
                            (   (l.{LeasesTableAttemptCountColumnName} IS NULL)
                                OR
                                (l.{LeasesTableAttemptCountColumnName} between {InitialValueAttemptCount} and {MaxChangeProcessAttemptCount})
                            )
                            ;
                        ";

            return new MySqlCommand(countUnprocessedQuery, connection, transaction);
        }

        /// <summary>
        /// Builds the query to update LastPolledTime in globalstate table (<see cref="RunChangeConsumptionLoopAsync()"/>).
        /// </summary>
        /// <param name="connection">The connection to add to the returned MySqlCommand</param>
        /// <param name="transaction">The transaction to add to the returned MySqlCommand</param>
        /// <param name="newLastPolledTime">The last polled time to be updated</param>
        /// <returns>The MySqlCommand populated with the query and appropriate parameters</returns>
        private MySqlCommand BuildUpdateGlobalStateTableLastPollingTime(MySqlConnection connection, MySqlTransaction transaction, string newLastPolledTime)
        {
            string updateLastPollingTimeQuery = $@"UPDATE {GlobalStateTableName}
                        SET {GlobalStateTableLastPolledTimeColumnName} = TIMESTAMP('{newLastPolledTime}')
                        where {GlobalStateTableUserFunctionIDColumnName} = '{this._userFunctionId}' AND {GlobalStateTableUserTableIDColumnName} = '{this._userTableId}';";

            return new MySqlCommand(updateLastPollingTimeQuery, connection, transaction);
        }

        /// <summary>
        /// 
        /// Builds the query to delete rows from lease table which processed completed (<see cref="RunChangeConsumptionLoopAsync()"/>).
        /// </summary>
        /// <param name="connection">The connection to add to the returned MySqlCommand</param>
        /// <param name="transaction">The transaction to add to the returned MySqlCommand</param>
        /// <returns>The MySqlCommand populated with the query and appropriate parameters</returns>
        private MySqlCommand BuildDeleteProcessedChangesInLeaseTable(MySqlConnection connection, MySqlTransaction transaction)
        {
            string deleteProcessedChangesQuery = $@"DELETE FROM {this._leasesTableName} 
                        where   ({LeasesTableLeaseExpirationTimeColumnName} IS NULL 
                                 OR
                                 ({LeasesTableAttemptCountColumnName} = 5 AND {LeasesTableLeaseExpirationTimeColumnName} < {MYSQL_FUNC_CURRENTTIME})
                                );";

            return new MySqlCommand(deleteProcessedChangesQuery, connection, transaction);
        }

        private enum State
        {
            CheckingForChanges,
            ProcessingChanges,
            Cleanup
        }
    }
}
