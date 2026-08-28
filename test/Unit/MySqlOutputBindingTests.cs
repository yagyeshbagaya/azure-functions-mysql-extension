// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using MySql.Data.MySqlClient;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.MySql.Tests.Unit
{
    public class MySqlOutputBindingTests
    {
        private static readonly Mock<IConfiguration> config = new();
        private static readonly Mock<ILogger> logger = new();

        [Fact]
        public void TestNullCollectorConstructorArguments()
        {
            var arg = new MySqlAttribute(string.Empty, "MySqlConnectionString");
            Assert.Throws<ArgumentNullException>(() => new MySqlAsyncCollector<string>(config.Object, null, logger.Object));
            Assert.Throws<ArgumentNullException>(() => new MySqlAsyncCollector<string>(null, arg, logger.Object));
        }

        [Theory]
        [InlineData("mydb.Products", "mydb", "`mydb`", "'mydb'", "Products", "`Products`", "'Products'", "mydb.Products", "`mydb`.`Products`")]
        [InlineData("`mydb`.Products", "mydb", "`mydb`", "'mydb'", "Products", "`Products`", "'Products'", "mydb.Products", "`mydb`.`Products`")]
        [InlineData("mydb.`Products`", "mydb", "`mydb`", "'mydb'", "Products", "`Products`", "'Products'", "mydb.Products", "`mydb`.`Products`")]
        [InlineData("`mydb`.`Products`", "mydb", "`mydb`", "'mydb'", "Products", "`Products`", "'Products'", "mydb.Products", "`mydb`.`Products`")]
        [InlineData("Products", "SCHEMA()", "SCHEMA()", "SCHEMA()", "Products", "`Products`", "'Products'", "Products", "`Products`")]
        [InlineData("`Products`", "SCHEMA()", "SCHEMA()", "SCHEMA()", "Products", "`Products`", "'Products'", "Products", "`Products`")]
        [InlineData("`Products'`", "SCHEMA()", "SCHEMA()", "SCHEMA()", "Products'", "`Products'`", "'Products\\''", "Products'", "`Products'`")]
        [InlineData("`Products\\'`", "SCHEMA()", "SCHEMA()", "SCHEMA()", "Products\\'", "`Products\\'`", "'Products\\\\\\''", "Products\\'", "`Products\\'`")]
        [InlineData("`''`", "SCHEMA()", "SCHEMA()", "SCHEMA()", "''", "`''`", "'\\'\\''", "''", "`''`")]
        public void TestMySqlObject(string fullName,
            string expectedSchema,
            string expectedAcuteQuotedSchema,
            string expectedSingleQuotedSchema,
            string expectedName,
            string expectedAcuteQuotedName,
            string expectedSingleQuotedName,
            string expectedFullName,
            string expectedAcuteQuotedFullName)
        {
            var MySqlObj = new MySqlObject(fullName);
            Assert.Equal(expectedSchema, MySqlObj.Schema);
            Assert.Equal(expectedAcuteQuotedSchema, MySqlObj.AcuteQuotedSchema);
            Assert.Equal(expectedSingleQuotedSchema, MySqlObj.SingleQuotedSchema);
            Assert.Equal(expectedName, MySqlObj.Name);
            Assert.Equal(expectedAcuteQuotedName, MySqlObj.AcuteQuotedName);
            Assert.Equal(expectedSingleQuotedName, MySqlObj.SingleQuotedName);
            Assert.Equal(expectedFullName, MySqlObj.FullName);
            Assert.Equal(expectedAcuteQuotedFullName, MySqlObj.AcuteQuotedFullName);
        }

        [Theory]
        [InlineData("'mydb'.'Products'", "Encountered error while parsing object name:")]
        [InlineData("\"mydb\".\"Products\"", "Encountered error while parsing object name:")]
        [InlineData("'Products'", "Encountered error while parsing object name:")]
        public void TestMySqlObjectParseError(string fullName, string expectedErrorMessage)
        {
            string errorMessage = Assert.Throws<InvalidOperationException>(() => new MySqlObject(fullName)).Message;
            Assert.StartsWith(expectedErrorMessage, errorMessage);
        }

        [Theory]
        [InlineData("columnName", "`columnName`")]
        [InlineData("mydb.tablename", "`mydb.tablename`")]
        public void TestAsAcuteQuotedString(string s, string expectedResult)
        {
            string result = s.AsAcuteQuotedString();
            Assert.Equal(expectedResult, result);
        }

        [Theory]
        // Ordinary values are left unchanged.
        [InlineData("Hello", "Hello")]
        [InlineData("", "")]
        // Single quotes are escaped (' => \') so they cannot terminate a SQL string literal.
        [InlineData("O'Brien", "O\\'Brien")]
        [InlineData("a'b'c", "a\\'b\\'c")]
        [InlineData("'", "\\'")]
        [InlineData("''", "\\'\\'")]
        [InlineData("'abc", "\\'abc")]
        [InlineData("abc'", "abc\\'")]
        // Backslashes are escaped first (\ => \\) so an escaped quote cannot be "un-escaped".
        [InlineData("a\\b", "a\\\\b")]
        [InlineData("\\", "\\\\")]
        [InlineData("\\\\", "\\\\\\\\")]
        [InlineData("\\'", "\\\\\\'")]
        // Double quotes, backticks, wildcards and control characters are NOT altered
        // (only ' and \ are escaped by this function).
        [InlineData("\"", "\"")]
        [InlineData("`", "`")]
        [InlineData("%_", "%_")]
        [InlineData("a\nb", "a\nb")]
        [InlineData("na\u00efve", "na\u00efve")]
        // The CWE-89 SQL-injection payload is neutralized: the breaking quote is escaped.
        [InlineData("x', 9.99); DELETE FROM Products WHERE Cost > 100; -- ", "x\\', 9.99); DELETE FROM Products WHERE Cost > 100; -- ")]
        [InlineData("'; DROP TABLE Users; --", "\\'; DROP TABLE Users; --")]
        // Classic authentication/tautology injection payloads.
        [InlineData("' OR '1'='1", "\\' OR \\'1\\'=\\'1")]
        [InlineData("' OR 1=1 -- ", "\\' OR 1=1 -- ")]
        [InlineData("admin'--", "admin\\'--")]
        [InlineData("1'; UPDATE Products SET Cost=0; -- ", "1\\'; UPDATE Products SET Cost=0; -- ")]
        // Whitespace / control characters other than quote and backslash pass through untouched.
        [InlineData("a\tb", "a\tb")]
        [InlineData("a\r\nb", "a\r\nb")]
        [InlineData("\0", "\0")]
        public void TestAsSingleQuoteEscapedString(string input, string expected)
        {
            Assert.Equal(expected, input.AsSingleQuoteEscapedString());
        }

        [Theory]
        // The value is wrapped in single quotes.
        [InlineData("Hello", "'Hello'")]
        [InlineData("", "''")]
        // Any embedded quotes/backslashes are escaped inside those quotes.
        [InlineData("O'Brien", "'O\\'Brien'")]
        [InlineData("'", "'\\''")]
        [InlineData("''", "'\\'\\''")]
        [InlineData("a\\b", "'a\\\\b'")]
        [InlineData("\\", "'\\\\'")]
        // Double quotes and non-quote characters are wrapped but otherwise unchanged.
        [InlineData("\"", "'\"'")]
        [InlineData("a\nb", "'a\nb'")]
        [InlineData("na\u00efve", "'na\u00efve'")]
        // The injected statement stays inside the string literal instead of breaking out of it.
        [InlineData("x', 9.99); DELETE FROM Products WHERE Cost > 100; -- ", "'x\\', 9.99); DELETE FROM Products WHERE Cost > 100; -- '")]
        [InlineData("'; DROP TABLE Users; --", "'\\'; DROP TABLE Users; --'")]
        [InlineData("' OR '1'='1", "'\\' OR \\'1\\'=\\'1'")]
        [InlineData("admin'--", "'admin\\'--'")]
        public void TestAsSingleQuotedString(string input, string expected)
        {
            Assert.Equal(expected, input.AsSingleQuotedString());
        }

        /// <summary>
        /// POCO used to exercise the real value-building path in
        /// <see cref="MySqlAsyncCollector{T}"/> (the Fix 1 code path).
        /// </summary>
        private class UpsertTestProduct
        {
            public int ProductId { get; set; }

            public string Name { get; set; }
        }

        [Theory]
        // Ordinary value.
        [InlineData("Widget")]
        // A legitimate apostrophe is bound as-is (no escaping needed, no broken SQL).
        [InlineData("O'Brien")]
        // The CWE-89 payload is bound verbatim as a parameter, so it can never execute as SQL.
        [InlineData("x', 9.99); DELETE FROM Products WHERE Cost > 100; -- ")]
        public void TestGetColValuesForUpsertParameterizesStringValues(string name)
        {
            // Arrange: build the TableInformation the collector needs (ProductId + Name columns).
            var columns = new Dictionary<string, string>
            {
                { "ProductId", "int" },
                { "Name", "varchar(100)" },
            };
            var primaryKeys = new List<PrimaryKey> { new("ProductId", true, false) };
            IEnumerable<PropertyInfo> primaryKeyProperties = typeof(UpsertTestProduct).GetProperties()
                .Where(p => p.Name == "ProductId");

            var tableInfo = new MySqlAsyncCollector<UpsertTestProduct>.TableInformation(
                primaryKeys, primaryKeyProperties, columns, hasIdentityColumnPrimaryKeys: true);

            var row = new UpsertTestProduct { ProductId = 1, Name = name };
            var parameters = new List<MySqlParameter>();

            // GetColValuesForUpsert is private static; invoke it via reflection to test the real fix.
            MethodInfo method = typeof(MySqlAsyncCollector<UpsertTestProduct>)
                .GetMethod("GetColValuesForUpsert", BindingFlags.NonPublic | BindingFlags.Static);

            // Act
            string result = (string)method.Invoke(
                null,
                new object[] { row, tableInfo, new[] { "ProductId", "Name" }, parameters });

            // Assert: the generated VALUES fragment contains only parameter placeholders - no literal data,
            // so an attacker-controlled value cannot break out of the SQL.
            Assert.Equal("(@p0, @p1)", result);
            Assert.Equal(2, parameters.Count);
            // ProductId is bound as its numeric value.
            Assert.Equal(1L, (long)parameters[0].Value);
            // The (possibly malicious) name is bound verbatim - stored as data, never executed as SQL.
            Assert.Equal(name, parameters[1].Value);
        }
    }
}
