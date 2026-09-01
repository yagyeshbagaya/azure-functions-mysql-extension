// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Extensions.MySql.Samples.Common;
using Microsoft.Azure.WebJobs.Extensions.MySql.Samples.TriggerBindingSamples;
using Microsoft.Azure.WebJobs.Extensions.MySql.Tests.Common;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json.Linq;
using xRetry;
using Xunit;
using Xunit.Abstractions;
using static Microsoft.Azure.WebJobs.Extensions.MySql.MySqlTriggerConstants;

namespace Microsoft.Azure.WebJobs.Extensions.MySql.Tests.Integration
{
    [Collection(IntegrationTestsCollection.Name)]
    public class MySqlTriggerBindingIntegrationTests : MySqlTriggerBindingIntegrationTestBase
    {
        public MySqlTriggerBindingIntegrationTests(ITestOutputHelper output = null) : base(output)
        {
        }

        /// <summary>
        /// Ensures that the user function gets invoked for each of the insert, update and delete operation.
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public async Task SingleOperationTriggerTest(SupportedLanguages lang)
        {
            this.SetChangeTrackingForTable("Products");
            this.StartFunctionHost(nameof(ProductsTrigger), lang);

            int firstId = 1;
            int lastId = 30;
            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () => { this.InsertProducts(firstId, lastId); return Task.CompletedTask; },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId));

            firstId = 1;
            lastId = 20;
            // All table columns (not just the columns that were updated) would be returned for update operation.
            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () => { this.UpdateProducts(firstId, lastId); return Task.CompletedTask; },
                id => $"Updated Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId));
        }

        /// <summary>
        /// Verifies that manually settings of the batch size and polling interval from the app settings overrides settings from the host options
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public async Task ConfigOverridesHostOptionsTest(SupportedLanguages lang)
        {
            string extensionPath = "AzureWebJobs:Extensions:MySql";
            var values = new Dictionary<string, string>
            {
                { $"{extensionPath}:BatchSize", "30" },
                { $"{extensionPath}:PollingIntervalMs", "1000" },
                { $"{extensionPath}:MaxChangesPerWorker", "100" },
            };

            MySqlOptions options = TestHelpers.GetConfiguredOptions<MySqlOptions>(b =>
            {
                b.AddMySql();
            }, values);
            // Use enough items to require 4 batches to be processed but then
            // set the max batch size to the same value so they can all be processed in one
            // batch. The test will only wait for ~1 batch worth of time so will timeout
            // if the max batch size isn't actually changed
            const int maxBatchSize = MySqlOptions.DefaultMaxBatchSize * 4;
            const int pollingIntervalMs = MySqlOptions.DefaultPollingIntervalMs / 2;
            const int firstId = 1;
            const int lastId = maxBatchSize;
            this.SetChangeTrackingForTable("Products");
            var taskCompletionSource = new TaskCompletionSource<bool>();
            DataReceivedEventHandler handler = TestUtils.CreateOutputReceievedHandler(
                taskCompletionSource,
                @"Starting change consumption loop. MaxBatchSize: (\d*) PollingIntervalMs: \d*",
                "MaxBatchSize",
                maxBatchSize.ToString());
            this.StartFunctionHost(
                nameof(ProductsTriggerWithValidation),
                lang,
                useTestFolder: true,
                customOutputHandler: handler,
                environmentVariables: new Dictionary<string, string>() {
                    { "TEST_EXPECTED_MAX_BATCH_SIZE", maxBatchSize.ToString() },
                    { "MySql_Trigger_BatchSize", maxBatchSize.ToString() }, // Use old BatchSize config
                    { "MySql_Trigger_PollingIntervalMs", pollingIntervalMs.ToString() }
                }
            );

            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () => { this.InsertProducts(firstId, lastId); return Task.CompletedTask; },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId, maxBatchSize: maxBatchSize, pollingIntervalMs: pollingIntervalMs));

            await taskCompletionSource.Task.TimeoutAfter(TimeSpan.FromSeconds(5), "Timed out waiting for MaxBatchSize and PollingInterval configuration message");
        }

        /// <summary>
        /// Verifies that manually setting the max batch size correctly changes the number of changes processed at once
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public async Task MaxBatchSizeOverrideTriggerTest(SupportedLanguages lang)
        {
            // Use enough items to require 4 batches to be processed but then
            // set the max batch size to the same value so they can all be processed in one
            // batch. The test will only wait for ~1 batch worth of time so will timeout
            // if the max batch size isn't actually changed
            const int maxBatchSize = MySqlOptions.DefaultMaxBatchSize * 4;
            const int firstId = 1;
            const int lastId = maxBatchSize;
            this.SetChangeTrackingForTable("Products");
            var taskCompletionSource = new TaskCompletionSource<bool>();
            DataReceivedEventHandler handler = TestUtils.CreateOutputReceievedHandler(
                taskCompletionSource,
                @"Starting change consumption loop. MaxBatchSize: (\d*) PollingIntervalMs: \d*",
                "MaxBatchSize",
                maxBatchSize.ToString());
            this.StartFunctionHost(
                nameof(ProductsTriggerWithValidation),
                lang,
                useTestFolder: true,
                customOutputHandler: handler,
                environmentVariables: new Dictionary<string, string>() {
                    { "TEST_EXPECTED_MAX_BATCH_SIZE", maxBatchSize.ToString() },
                    { "MySql_Trigger_MaxBatchSize", maxBatchSize.ToString() },
                }
            );

            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () => { this.InsertProducts(firstId, lastId); return Task.CompletedTask; },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId, maxBatchSize: maxBatchSize));
            await taskCompletionSource.Task.TimeoutAfter(TimeSpan.FromSeconds(5), "Timed out waiting for MaxBatchSize configuration message");
        }

        /// <summary>
        /// Verifies that manually setting the polling interval correctly changes the delay between processing each batch of changes
        /// </summary>
        [RetryTheory(Skip = "Test skipped temporary")]
        [MySqlInlineData()]
        public async Task PollingIntervalOverrideTriggerTest(SupportedLanguages lang)
        {
            const int firstId = 1;
            // Use enough items to require 5 batches to be processed - the test will
            // only wait for the expected time and timeout if the default polling
            // interval isn't actually modified.
            const int lastId = MySqlOptions.DefaultMaxBatchSize * 5;
            const int pollingIntervalMs = MySqlOptions.DefaultPollingIntervalMs / 2;
            this.SetChangeTrackingForTable("Products");
            var taskCompletionSource = new TaskCompletionSource<bool>();
            DataReceivedEventHandler handler = TestUtils.CreateOutputReceievedHandler(
                taskCompletionSource,
                @"Starting change consumption loop. MaxBatchSize: \d* PollingIntervalMs: (\d*)",
                "PollingInterval",
                pollingIntervalMs.ToString());
            this.StartFunctionHost(
                nameof(ProductsTriggerWithValidation),
                lang,
                useTestFolder: true,
                customOutputHandler: handler,
                environmentVariables: new Dictionary<string, string>() {
                    { "MySql_Trigger_PollingIntervalMs", pollingIntervalMs.ToString() }
                }
            );

            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () => { this.InsertProducts(firstId, lastId); return Task.CompletedTask; },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId, pollingIntervalMs: pollingIntervalMs));
            await taskCompletionSource.Task.TimeoutAfter(TimeSpan.FromSeconds(5), "Timed out waiting for PollingInterval configuration message");
        }

        /// <summary>
        /// Verifies that if several changes have happened to the table row since last invocation, then a single net
        /// change for that row is passed to the user function.
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public async Task MultiOperationTriggerTest(SupportedLanguages lang)
        {
            int firstId = 1;
            int lastId = 5;
            // Use a long polling interval so that each group of operations below (insert + updates) all
            // occur within a single poll window. This makes the trigger report a single coalesced net
            // change (the latest row state) instead of racing the default 1s poll and catching an
            // intermediate state, which made this test flaky on slower/loaded CI agents.
            const int pollingIntervalMs = 10000;
            this.SetChangeTrackingForTable("Products");
            this.StartFunctionHost(
                nameof(ProductsTrigger),
                lang,
                environmentVariables: new Dictionary<string, string>()
                {
                    { "MySql_Trigger_PollingIntervalMs", pollingIntervalMs.ToString() }
                });

            // 1. Insert + multiple updates to a row are treated as single insert with latest row values.
            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () =>
                {
                    this.InsertProducts(firstId, lastId);
                    this.UpdateProducts(firstId, lastId);
                    this.UpdateProducts(firstId, lastId);
                    return Task.CompletedTask;
                },
                id => $"Updated Updated Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId, pollingIntervalMs: pollingIntervalMs));

            firstId = 6;
            lastId = 10;
            // 2. Multiple updates to a row are treated as single update with latest row values.
            // First insert items and wait for those changes to be sent
            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () =>
                {
                    this.InsertProducts(firstId, lastId);
                    return Task.CompletedTask;
                },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId, pollingIntervalMs: pollingIntervalMs));

            firstId = 6;
            lastId = 10;
            // Now do multiple updates at once and verify the updates are batched together
            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () =>
                {
                    this.UpdateProducts(firstId, lastId);
                    this.UpdateProducts(firstId, lastId);
                    return Task.CompletedTask;
                },
                id => $"Updated Updated Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId, pollingIntervalMs: pollingIntervalMs));
        }

        /// <summary>
        /// Ensures correct functionality with multiple user functions tracking the same table.
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public async Task MultiFunctionTriggerTest(SupportedLanguages lang)
        {
            const string Trigger1Changes = "Trigger1 Changes: ";
            const string Trigger2Changes = "Trigger2 Changes: ";

            this.SetChangeTrackingForTable("Products");

            string functionList = $"{nameof(MultiFunctionTrigger.MultiFunctionTrigger1)} {nameof(MultiFunctionTrigger.MultiFunctionTrigger2)}";
            this.StartFunctionHost(functionList, lang, useTestFolder: true);

            // 1. INSERT
            int firstId = 1;
            int lastId = 30;
            // Set up monitoring for Trigger 1...
            Task changes1Task = this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () =>
                {
                    return Task.CompletedTask;
                },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId),
                Trigger1Changes
                );

            // Set up monitoring for Trigger 2...
            Task changes2Task = this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () =>
                {
                    return Task.CompletedTask;
                },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId),
                Trigger2Changes
                );

            // Now that monitoring is set up make the changes and then wait for the monitoring tasks to see them and complete
            this.InsertProducts(firstId, lastId);
            await Task.WhenAll(changes1Task, changes2Task);

            // 2. UPDATE
            firstId = 1;
            lastId = 20;
            // All table columns (not just the columns that were updated) would be returned for update operation.
            // Set up monitoring for Trigger 1...
            changes1Task = this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () =>
                {
                    return Task.CompletedTask;
                },
                id => $"Updated Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId),
                Trigger1Changes);

            // Set up monitoring for Trigger 2...
            changes2Task = this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () =>
                {
                    return Task.CompletedTask;
                },
                id => $"Updated Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId),
                Trigger2Changes);

            // Now that monitoring is set up make the changes and then wait for the monitoring tasks to see them and complete
            this.UpdateProducts(firstId, lastId);
            await Task.WhenAll(changes1Task, changes2Task);
        }

        /// <summary>
        /// Ensures correct functionality with user functions running across multiple functions host processes. temproaly
        /// </summary>
        [RetryTheory(Skip = "Test skipped temporary")]
        [MySqlInlineData()]
        public async Task MultiHostTriggerTest(SupportedLanguages lang)
        {
            this.SetChangeTrackingForTable("Products");

            // Prepare three function host processes.
            this.StartFunctionHost(nameof(ProductsTrigger), lang);
            this.StartFunctionHost(nameof(ProductsTrigger), lang);
            this.StartFunctionHost(nameof(ProductsTrigger), lang);

            int firstId = 1;
            int lastId = 90;
            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () => { this.InsertProducts(firstId, lastId); return Task.CompletedTask; },
                id => $"Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId));

            firstId = 1;
            lastId = 60;
            // All table columns (not just the columns that were updated) would be returned for update operation.
            await this.WaitForProductChanges(
                firstId,
                lastId,
                MySqlChangeOperation.Update,
                () => { this.UpdateProducts(firstId, lastId); return Task.CompletedTask; },
                id => $"Updated Product {id}",
                id => id * 100,
                this.GetBatchProcessingTimeout(firstId, lastId));
        }

        /// <summary>
        /// Tests the error message when the user table is not present in the database.
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public void TableNotPresentTriggerTest(SupportedLanguages lang)
        {
            this.StartFunctionHostAndWaitForError(
                nameof(TableNotPresentTrigger),
                lang,
                true,
                "Could not find the specified table in the database.");
        }

        /// <summary>
        /// Tests the error message when the user table does not contain primary key.
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public void PrimaryKeyNotCreatedTriggerTest(SupportedLanguages lang)
        {
            this.SetChangeTrackingForTable("ProductsWithoutPrimaryKey");

            this.StartFunctionHostAndWaitForError(
                nameof(PrimaryKeyNotPresentTrigger),
                lang,
                true,
                "Could not find primary key(s) for the given table.");
        }

        /// <summary>
        /// Tests the error message when the user table contains columns of unsupported MySQL types.
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public void UnsupportedColumnTypesTriggerTest(SupportedLanguages lang)
        {
            this.SetChangeTrackingForTable("ProductsWithUnsupportedColumnTypes");

            this.StartFunctionHostAndWaitForError(
                nameof(UnsupportedColumnTypesTrigger),
                lang,
                true,
                "Found column(s) with unsupported type(s): " +
                "'Point' (type: point), 'MultiPoint' (type: multipoint), " +
                "'LineString' (type: linestring), 'MultiLineString' (type: multilinestring), " +
                "'Polygon' (type: polygon), 'MultiPolygon' (type: multipolygon), " +
                "'Geometry' (type: geometry), 'GeometryCollection' (type: geometrycollection)" +
                " in table: 'ProductsWithUnsupportedColumnTypes'.");
        }

        /// <summary>
        /// Tests the error message when change tracking is not enabled on the user table.
        /// </summary>
        [RetryTheory]
        [MySqlInlineData()]
        public void ChangeTrackingNotEnabledTriggerTest(SupportedLanguages lang)
        {
            this.StartFunctionHostAndWaitForError(
                nameof(ProductsTrigger),
                lang,
                false,
                $"The specified table does not have the column named '{UpdateAtColumnName}', hence trigger binding cannot be created on this table.");
        }

        /// <summary>
        /// Tests that the GetMetrics call works correctly.
        /// </summary>
        /// <remarks>We call this directly since there isn't a way to test scaling locally - with this we at least verify the methods called don't throw unexpectedly.</remarks>
        [Fact]
        public async Task GetMetricsTest()
        {
            this.SetChangeTrackingForTable("Products");
            string userFunctionId = "func-id";
            IConfiguration configuration = new ConfigurationBuilder().Build();
            var listener = new MySqlTriggerListener<Product>(this.DbConnectionString, "Products", "", userFunctionId, Mock.Of<ITriggeredFunctionExecutor>(), Mock.Of<MySqlOptions>(), Mock.Of<ILogger>(), configuration);
            await listener.StartAsync(CancellationToken.None);
            // Cancel immediately so the listener doesn't start processing the changes
            await listener.StopAsync(CancellationToken.None);
            var metricsProvider = new MySqlTriggerMetricsProvider(this.DbConnectionString, Mock.Of<ILogger>(), new MySqlObject("Products"), userFunctionId, "");
            MySqlTriggerMetrics metrics = await metricsProvider.GetMetricsAsync();
            Assert.True(metrics.UnprocessedChangeCount == 0, "There should initially be 0 unprocessed changes");
            this.InsertProducts(1, 5);
            metrics = await metricsProvider.GetMetricsAsync();
            Assert.True(metrics.UnprocessedChangeCount == 5, $"There should be 5 unprocessed changes after insertion. Actual={metrics.UnprocessedChangeCount}");
        }

        /// <summary>
        /// Tests that the Scale Controller is able to scale out the workers when required.
        /// </summary>
        [Fact]
        public async Task ScaleHostEndToEndTest()
        {
            string TestFunctionName = "TestFunction";
            string ConnectionStringName = "MySqlConnectionString";
            IConfiguration configuration = new ConfigurationBuilder().Build();

            string hostJson =
            /*lang = json */
                          @"{
                ""azureWebJobs"" : {
                    ""extensions"": {
                        ""mysql"": {
                            ""MaxChangesPerWorker"" :  10
                        }
                    }
                }   
            }";
            string mysqlTriggerJson = $@"{{
                    ""name"": ""{TestFunctionName}"",
                    ""type"": ""mysqlTrigger"",
                    ""tableName"": ""Products"",
                    ""connectionStringSetting"": ""{ConnectionStringName}"",
                    ""userFunctionId"" : ""testFunctionId""
                }}";
            var triggerMetadata = new TriggerMetadata(JObject.Parse(mysqlTriggerJson));

            this.SetChangeTrackingForTable("Products");

            // Initializing the listener is needed to create relevant lease table to get unprocessed changes. 
            // We would be using the scale host methods to get the scale status so the configuration values are not needed here.
            var listener = new MySqlTriggerListener<Product>(this.DbConnectionString, "Products", "", "testFunctionId", Mock.Of<ITriggeredFunctionExecutor>(), Mock.Of<MySqlOptions>(), Mock.Of<ILogger>(), configuration);
            await listener.StartAsync(CancellationToken.None);
            // Cancel immediately so the listener doesn't start processing the changes
            await listener.StopAsync(CancellationToken.None);

            IHost host = new HostBuilder().ConfigureServices(services => services.AddAzureClientsCore()).Build();
            AzureComponentFactory defaultAzureComponentFactory = host.Services.GetService<AzureComponentFactory>();

            string hostId = "test-host";
            var loggerProvider = new TestLoggerProvider();

            IHostBuilder hostBuilder = new HostBuilder();
            hostBuilder.ConfigureLogging(configure =>
            {
                configure.SetMinimumLevel(LogLevel.Debug);
                configure.AddProvider(loggerProvider);
            });
            hostBuilder.ConfigureAppConfiguration((hostBuilderContext, config) =>
            {
                // Adding host.json here
                config.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(hostJson)));

                var settings = new Dictionary<string, string>()
                {
                    { ConnectionStringName, this.DbConnectionString },
                    { "Microsoft.Azure.WebJobs.Extensions.MySql", "1" }
                };

                // Adding app setting
                config.AddInMemoryCollection(settings);
            })
            .ConfigureServices(services =>
            {
                services.AddAzureClientsCore();
                services.AddAzureStorageScaleServices();
            })
            .ConfigureWebJobsScale((context, builder) =>
            {
                builder.AddMySql();
                builder.UseHostId(hostId);
                builder.AddMySqlScaleForTrigger(triggerMetadata);
            },
            scaleOptions =>
            {
                scaleOptions.IsTargetScalingEnabled = true;
                scaleOptions.MetricsPurgeEnabled = false;
                scaleOptions.ScaleMetricsMaxAge = TimeSpan.FromMinutes(4);
                scaleOptions.IsRuntimeScalingEnabled = true;
                scaleOptions.ScaleMetricsSampleInterval = TimeSpan.FromSeconds(1);
            });

            IHost scaleHost = hostBuilder.Build();
            await scaleHost.StartAsync();

            int firstId = 1;
            int lastId = 30;
            this.InsertProducts(firstId, lastId);

            IScaleStatusProvider scaleManager = scaleHost.Services.GetService<IScaleStatusProvider>();
            AggregateScaleStatus scaleStatus = await scaleManager.GetScaleStatusAsync(new ScaleStatusContext());

            Assert.Equal(ScaleVote.ScaleOut, scaleStatus.Vote);
            Assert.Equal(3, scaleStatus.TargetWorkerCount);

            await scaleHost.StopAsync();

        }

        /// <summary>
        /// Tests that the GlobalState table has LastPolledTime column.
        /// </summary>
        /// <remarks>We call StartAsync which initializes the GlobalState, then check if the GlobalState has the column.</remarks>
        [Fact]
        public async Task GlobalStateTableLastPolledTimeColumn_Exist_OnStartup()
        {
            this.SetChangeTrackingForTable("Products");
            string userFunctionId = "func-id";
            IConfiguration configuration = new ConfigurationBuilder().Build();
            var listener = new MySqlTriggerListener<Product>(this.DbConnectionString, "Products", "", userFunctionId, Mock.Of<ITriggeredFunctionExecutor>(), Mock.Of<MySqlOptions>(), Mock.Of<ILogger>(), configuration);
            await listener.StartAsync(CancellationToken.None);
            // Cancel immediately so the listener doesn't start processing the changes
            await listener.StopAsync(CancellationToken.None);
            //Check if {GlobalStateTableLastPolledTimeColumnName} column exists in the GlobalState table
            Assert.Equal(1, Convert.ToInt32(this.ExecuteScalar($@"SELECT 1 FROM information_schema.columns WHERE COLUMN_NAME = '{GlobalStateTableLastPolledTimeColumnName}' AND TABLE_SCHEMA = '{SchemaName}' AND TABLE_NAME = '{GlobalState}'")));
        }

        /// <summary>
        /// Ensures that all column types are serialized correctly.
        /// </summary>
        [Theory(Skip = "Test skipped temporary")]
        [MySqlInlineData()]
        public async Task ProductsColumnTypesTriggerTest(SupportedLanguages lang)
        {
            this.SetChangeTrackingForTable("ProductsColumnTypes");
            this.StartFunctionHost(nameof(ProductsColumnTypesTrigger), lang, true);
            ProductColumnTypes expectedResponse = Utils.JsonDeserializeObject<ProductColumnTypes>(/*lang=json,strict*/ "{\"ProductId\":999,\"BigIntType\":999,\"BitType\":1,\"DecimalType\":1.2345,\"NumericType\":1.2345,\"SmallIntType\":1,\"TinyIntType\":1,\"FloatType\":0.1,\"RealType\":0.1,\"DateType\":\"2022-10-20T00:00:00\",\"DatetimeType\":\"2022-10-20T12:39:13\",\"TimeType\":\"12:39:13\",\"CharType\":\"test\",\"VarcharType\":\"test\",\"NcharType\":\"test\",\"NvarcharType\":\"test\",\"BinaryType\":\"dGVzdA==\",\"VarbinaryType\":\"dGVzdA==\"}");
            int index = 0;
            string messagePrefix = "MySQL Changes: ";

            var taskCompletion = new TaskCompletionSource<bool>();

            void MonitorOutputData(object sender, DataReceivedEventArgs e)
            {
                if (e.Data != null && (index = e.Data.IndexOf(messagePrefix, StringComparison.Ordinal)) >= 0)
                {
                    string json = e.Data[(index + messagePrefix.Length)..];
                    // Sometimes we'll get messages that have extra logging content on the same line - so to prevent that from breaking
                    // the deserialization we look for the end of the changes array and only use that.
                    // (This is fine since we control what content is in the array so know that none of the items have a ] in them)
                    json = json[..(json.IndexOf(']') + 1)];
                    IReadOnlyList<MySqlChange<ProductColumnTypes>> changes;
                    try
                    {
                        changes = Utils.JsonDeserializeObject<IReadOnlyList<MySqlChange<ProductColumnTypes>>>(json);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Exception deserializing JSON content. Error={ex.Message} Json=\"{json}\"", ex);
                    }
                    Assert.Equal(MySqlChangeOperation.Update, changes[0].Operation); // Expected change operation
                    ProductColumnTypes product = changes[0].Item;
                    Assert.NotNull(product); // Product deserialized correctly
                    Assert.Equal(expectedResponse, product); // The product has the expected values
                    taskCompletion.SetResult(true);
                }
            }
            ;

            // Set up listener for the changes coming in
            foreach (Process functionHost in this.FunctionHostList)
            {
                functionHost.OutputDataReceived += MonitorOutputData;
            }

            // Now that we've set up our listener trigger the actions to monitor
            string datetime = "2022-10-20 12:39:13.123";
            this.ExecuteNonQuery("INSERT INTO ProductsColumnTypes " +
                "(ProductId, BigIntType, BitType, DecimalType, NumericType, SmallIntType, TinyIntType, " +
                "FloatType, RealType, DateType, DatetimeType, TimeType, " +
                "CharType, VarcharType, NcharType, NvarcharType, BinaryType, VarbinaryType)" +
                " VALUES (" +
                "999, " + // ProductId,
                "999, " + // BigIntType
                "1, " + // BitType
                "1.2345, " + // DecimalType
                "1.2345, " + // NumericType
                "1, " + // SmallIntType
                "1, " + // TinyIntType
                ".1, " + // FloatType
                ".1, " + // RealType
                $"DATE('{datetime}')," + // DateType
                $"'{datetime}'," + // DatetimeType
                $"TIME('{datetime}')," + // TimeType
                "'test', " + // CharType
                "'test', " + // VarcharType
                "'test', " + // NcharType
                "'test', " +  // NvarcharType
                "'test', " + // BinaryType
                "'test')"); // VarbinaryType

            // Now wait until either we timeout or we've gotten all the expected changes, whichever comes first
            this.LogOutput($"[{DateTime.UtcNow:u}] Waiting for Insert changes (10000ms)");
            await taskCompletion.Task.TimeoutAfter(TimeSpan.FromMilliseconds(10000), $"Timed out waiting for Insert changes.");

            // Unhook handler since we're done monitoring these changes so we aren't checking other changes done later
            foreach (Process functionHost in this.FunctionHostList)
            {
                functionHost.OutputDataReceived -= MonitorOutputData;
            }
        }

        /// <summary>
        /// Ensures that the user defined leasesTableName is used to create the 'LeasesTable' table.
        /// </summary>
        [Theory]
        [MySqlInlineData()]
        public void LeasesTableNameTest(SupportedLanguages lang)
        {
            this.ExecuteNonQuery($"DROP TABLE IF EXISTS {SchemaName}.LeasesTable");
            this.SetChangeTrackingForTable("Products");
            int count = Convert.ToInt32(this.ExecuteScalar($"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_NAME = 'LeasesTable' AND TABLE_SCHEMA = '{SchemaName}'"));
            Assert.Equal(0, count);
            this.StartFunctionHost(nameof(ProductsTriggerLeasesTableName), lang);
            Thread.Sleep(5000);
            Assert.Equal(1, Convert.ToInt32(this.ExecuteScalar($"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_NAME = 'LeasesTable' AND TABLE_SCHEMA = '{SchemaName}'")));
        }
    }
}