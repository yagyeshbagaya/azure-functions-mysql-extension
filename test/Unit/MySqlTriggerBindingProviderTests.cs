// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Triggers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using MySql.Data.MySqlClient;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.MySql.Tests.Unit
{
    public class MySqlTriggerBindingProviderTests
    {
        /// <summary>
        /// Verifies that <see cref="MySqlTableChangeMonitor{T}"/> binds primary-key values used in the
        /// lease statements as parameters (placeholders "@pk0", "@pk1", ...) rather than interpolating
        /// them into SQL. This is the fix for the CWE-89 second-order SQL injection (MSRC 138520):
        /// even an attacker-controlled primary-key value is stored as data and can never execute.
        /// </summary>
        [Theory]
        [InlineData("42")]
        [InlineData("A123")]
        // A stored primary-key value carrying a SQL payload (the MSRC second-order injection vector).
        [InlineData("x'); SELECT SLEEP(3); -- ")]
        public void AddPrimaryKeyParameter_ReturnsPlaceholderAndBindsValueVerbatim(object value)
        {
            var parameters = new List<MySqlParameter>();
            MethodInfo method = typeof(MySqlTableChangeMonitor<object>)
                .GetMethod("AddPrimaryKeyParameter", BindingFlags.NonPublic | BindingFlags.Static);

            string first = (string)method.Invoke(null, new object[] { parameters, value });
            string second = (string)method.Invoke(null, new object[] { parameters, value });

            // Unique, sequential placeholders are emitted into the SQL - never the literal value.
            Assert.Equal("@pk0", first);
            Assert.Equal("@pk1", second);
            Assert.Equal(2, parameters.Count);
            // The (possibly malicious) value is bound verbatim as a parameter - stored as data, never executed as SQL.
            Assert.Equal(value, parameters[0].Value);
            Assert.Equal(value, parameters[1].Value);
        }

        /// <summary>
        /// Verifies a null primary-key value is bound as SQL NULL (DBNull) rather than throwing or
        /// being interpolated.
        /// </summary>
        [Fact]
        public void AddPrimaryKeyParameter_NullValue_BindsDbNull()
        {
            var parameters = new List<MySqlParameter>();
            MethodInfo method = typeof(MySqlTableChangeMonitor<object>)
                .GetMethod("AddPrimaryKeyParameter", BindingFlags.NonPublic | BindingFlags.Static);

            string parameterName = (string)method.Invoke(null, new object[] { parameters, null });

            Assert.Equal("@pk0", parameterName);
            Assert.Single(parameters);
            Assert.Equal(DBNull.Value, parameters[0].Value);
        }

        /// <summary>
        /// Verifies that null trigger binding is returned if the trigger parameter in user function does not have
        /// <see cref="MySqlTriggerAttribute"/> applied.
        /// </summary>
        [Fact]
        public async Task TryCreateAsync_TriggerParameterWithoutAttribute_ReturnsNullBinding()
        {
            Type parameterType = typeof(IReadOnlyList<MySqlChange<object>>);
            ITriggerBinding binding = await CreateTriggerBindingAsync(parameterType, nameof(UserFunctionWithoutAttribute));
            Assert.Null(binding);
        }

        /// <summary>
        /// Verifies that <see cref="ArgumentException"/> is thrown if the <see cref="MySqlTriggerAttribute"/> applied on
        /// the trigger parameter does not have <see cref="MySqlTriggerAttribute.ConnectionStringSetting"/> property set.
        /// <see cref="MySqlTriggerAttribute"/> attribute applied.
        /// </summary>
        [Fact]
        public async Task TryCreateAsync_MissingConnectionString_ThrowsException()
        {
            Type parameterType = typeof(IReadOnlyList<MySqlChange<object>>);
            Task testCode() { return CreateTriggerBindingAsync(parameterType, nameof(UserFunctionWithoutConnectionString)); }
            ArgumentException exception = await Assert.ThrowsAsync<ArgumentNullException>(testCode);

            Assert.Equal(
                "Value cannot be null. (Parameter 'connectionStringSetting')",
                exception.Message);
        }

        /// <summary>
        /// Verifies that <see cref="InvalidOperationException"/> is thrown if the <see cref="MySqlTriggerAttribute"/> is
        /// applied on the trigger parameter of unsupported type.
        /// </summary>
        [Theory]
        [InlineData(typeof(object))]
        [InlineData(typeof(MySqlChange<object>))]
        [InlineData(typeof(IEnumerable<MySqlChange<object>>))]
        [InlineData(typeof(IReadOnlyList<object>))]
        [InlineData(typeof(IReadOnlyList<IReadOnlyList<object>>))]
        public async Task TryCreateAsync_InvalidTriggerParameterType_ThrowsException(Type parameterType)
        {
            Task testCode() { return CreateTriggerBindingAsync(parameterType, nameof(UserFunctionWithAttribute)); }
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(testCode);

            Assert.Equal(
                $"Can't bind MySqlTriggerAttribute to type {parameterType}, this is not a supported type.",
                exception.Message);
        }

        /// <summary>
        /// Verifies that <see cref="MySqlTriggerBinding{T}"/> is returned if the <see cref="MySqlTriggerAttribute"/> has all
        /// required properties set and it is applied on the trigger parameter of supported type.
        /// </summary>
        [Fact]
        public async Task TryCreateAsync_ValidTriggerParameterType_ReturnsTriggerBinding()
        {
            Type parameterType = typeof(IReadOnlyList<MySqlChange<object>>);
            ITriggerBinding binding = await CreateTriggerBindingAsync(parameterType, nameof(UserFunctionWithAttribute));
            Assert.IsType<MySqlTriggerBinding<object>>(binding);
        }

        /// <summary>
        /// Verifies that <see cref="MySqlTriggerBinding{T}"/> is returned if the <see cref="MySqlTriggerAttribute"/> has all
        /// required and optional properties set and it is applied on the trigger parameter of supported type.
        /// </summary>
        [Fact]
        public async Task TryCreateAsync_LeasesTableName_ReturnsTriggerBinding()
        {
            Type parameterType = typeof(IReadOnlyList<MySqlChange<object>>);
            ITriggerBinding binding = await CreateTriggerBindingAsync(parameterType, nameof(UserFunctionWithLeasesTableName));
            Assert.IsType<MySqlTriggerBinding<object>>(binding);
        }

        private static async Task<ITriggerBinding> CreateTriggerBindingAsync(Type parameterType, string methodName)
        {
            var provider = new MySqlTriggerBindingProvider(
                Mock.Of<IConfiguration>(c => c["testConnectionStringSetting"] == "testConnectionString"),
                Mock.Of<ILoggerFactory>(f => f.CreateLogger(It.IsAny<string>()) == Mock.Of<ILogger>()),
                Mock.Of<Microsoft.Extensions.Options.IOptions<MySqlOptions>>());

            // Possibly the simplest way to construct a ParameterInfo object.
            ParameterInfo parameter = typeof(MySqlTriggerBindingProviderTests)
                .GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static)
                .MakeGenericMethod(parameterType)
                .GetParameters()[0];

            return await provider.TryCreateAsync(new TriggerBindingProviderContext(parameter, CancellationToken.None));
        }

        private static void UserFunctionWithoutAttribute<T>(T _) { }

        private static void UserFunctionWithoutConnectionString<T>([MySqlTrigger("testTableName", null)] T _) { }

        private static void UserFunctionWithAttribute<T>([MySqlTrigger("testTableName", "testConnectionStringSetting")] T _) { }

        private static void UserFunctionWithLeasesTableName<T>([MySqlTrigger("testTableName", "testConnectionStringSetting", "testLeasesTableName")] T _) { }
    }
}