using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.CSharp.RuntimeBinder;

namespace Dapper
{
    /// <summary>
    /// Main class for Dapper.SimpleCRUD extensions
    /// </summary>
    public static partial class SimpleCRUD
    {

        static SimpleCRUD()
        {
            SetDialect(_dialect);
        }

        private static Dialect _dialect = Dialect.SQLServer;
        private static string _encapsulation;
        private static string _getIdentitySql;
        private static string _getPagedListSql;

        private static readonly ConcurrentDictionary<Type, string> TableNames = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<string, string> ColumnNames = new ConcurrentDictionary<string, string>();
        private static ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>> IdPropertiesCache = new ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>>();
        private static ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>> ScaffoldablePropertiesCache = new ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>>();
        private static ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>> AllPropertiesCache = new ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>>();
        private static ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>> UpdateablePropertiesCache = new ConcurrentDictionary<string, IReadOnlyList<PropertyInfo>>();
        private static ConcurrentDictionary<string, string> NamedStringCache = new ConcurrentDictionary<string, string>();
        
        /// <summary>
        /// Return Cached Function Output Or Cache The Output Into A Typed Dictionary
        /// </summary>
        /// <typeparam name="T">Cache Dictionary Value Type</typeparam>
        /// <typeparam name="KeyType">Cache Dictionary Key Type</typeparam>
        /// <param name="key">Dictionary Key</param>
        /// <param name="dictionary">Ref Dictionary</param>
        /// <param name="valueLogic">Function To Evaluate To Generate & Cache The Value</param>
        /// <returns></returns>
        private static T ReturnCachedOrFuncValue<T, KeyType>(KeyType key, ConcurrentDictionary<KeyType, T> dictionary, Func<T> valueLogic)
        {
            if (dictionary.TryGetValue(key, out T value))
                return value;

            value = valueLogic();

            dictionary.AddOrUpdate(key, value, (t, v) => value);

            return value;
        }

        /// <summary>
        /// Append cached value to string builder
        /// </summary>
        /// <param name="sb"></param>
        /// <param name="key"></param>
        /// <param name="dictionary"></param>
        /// <param name="valueLogic"></param>
        private static void AppendToStringBuilderCachedOrFuncValue(StringBuilder sb, string key, ConcurrentDictionary<string, string> dictionary, Action<StringBuilder> valueLogic)
        {
            if (dictionary.TryGetValue(key, out string value))
            {
                sb.Append(value);
                return;
            }

            StringBuilder newSb = new StringBuilder();
            valueLogic(newSb);
            value = newSb.ToString();
            dictionary.AddOrUpdate(key, value, (t, v) => value);
            sb.Append(value);
        }


        private static ITableNameResolver _tableNameResolver = new TableNameResolver();
        private static IColumnNameResolver _columnNameResolver = new ColumnNameResolver();

        /// <summary>
        /// Returns the current dialect name
        /// </summary>
        /// <returns></returns>
        public static string GetDialect()
        {
            return _dialect.ToString();
        }

        /// <summary>
        /// Sets the database dialect 
        /// </summary>
        /// <param name="dialect"></param>
        public static void SetDialect(Dialect dialect)
        {
            switch (dialect)
            {
                case Dialect.PostgreSQL:
                    _dialect = Dialect.PostgreSQL;
                    _encapsulation = "\"{0}\"";
                    _getIdentitySql = "SELECT LASTVAL() AS id";
                    _getPagedListSql = "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})";
                    break;
                case Dialect.SQLite:
                    _dialect = Dialect.SQLite;
                    _encapsulation = "\"{0}\"";
                    _getIdentitySql = "SELECT LAST_INSERT_ROWID() AS id";
                    _getPagedListSql = "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})";
                    break;
                case Dialect.MySQL:
                    _dialect = Dialect.MySQL;
                    _encapsulation = "`{0}`";
                    _getIdentitySql = "SELECT LAST_INSERT_ID() AS id";
                    _getPagedListSql = "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {Offset},{RowsPerPage}";
                    break;
                default:
                    _dialect = Dialect.SQLServer;
                    _encapsulation = "[{0}]";
                    _getIdentitySql = "SELECT CAST(SCOPE_IDENTITY()  AS BIGINT) AS [id]";
                    _getPagedListSql = "SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY {OrderBy}) AS PagedNumber, {SelectColumns} FROM {TableName} {WhereClause}) AS u WHERE PagedNUMBER BETWEEN (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage})";
                    break;
            }
        }

        /// <summary>
        /// Sets the table name resolver
        /// </summary>
        /// <param name="resolver">The resolver to use when requesting the format of a table name</param>
        public static void SetTableNameResolver(ITableNameResolver resolver)
        {
            _tableNameResolver = resolver;
        }

        /// <summary>
        /// Sets the column name resolver
        /// </summary>
        /// <param name="resolver">The resolver to use when requesting the format of a column name</param>
        public static void SetColumnNameResolver(IColumnNameResolver resolver)
        {
            _columnNameResolver = resolver;
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>By default filters on the Id column</para>
        /// <para>-Id column name can be overridden by adding an attribute on your primary key property [Key]</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a single entity by a single id from table T</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Returns a single entity by a single id from table T.</returns>
        public static T Get<T>(this IDbConnection connection, object id, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildGetQuery<T>(id, out DynamicParameters dynParms);
            return connection.Query<T>(sb.ToString(), dynParms, transaction, true, commandTimeout).FirstOrDefault();
        }

        private static StringBuilder BuildGetQuery<T>(object id, out DynamicParameters dynParms)
        {
            var currenttype = typeof(T);
            var idProps = GetIdProperties(currenttype);

            if (!idProps.Any())
                throw new ArgumentException("Get<T> only supports an entity with a [Key] or Id property");

            StringBuilder sb = new StringBuilder();
            var name = GetTableName(currenttype);
            sb.Append("Select ");
            //create a new empty instance of the type to get the base properties
            BuildSelect<T>(sb, GetScaffoldableProperties<T>());
            sb.AppendFormat(" from {0} where ", name);

            bool hasAnyProperty = idProps
                .Select(p => sb.AppendFormat("{0} = @{1} and", GetColumnName(p), p.Name).Length)
                .Count() != 0;
            if (hasAnyProperty)
                sb.Remove(sb.Length - 4, 4);

            dynParms = new DynamicParameters();
            if (idProps.Count == 1)
                dynParms.Add("@" + idProps.First().Name, id);
            else
            {
                foreach (var prop in idProps)
                    dynParms.Add("@" + prop.Name, id.GetType().GetProperty(prop.Name).GetValue(id, null));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Get<{currenttype}>: {sb} with Id: {id}");
            return sb;
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>whereConditions is an anonymous type to filter the results ex: new {Category = 1, SubCategory=2}</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Gets a list of entities with optional exact match where conditions</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildGetListQuery<T>(whereConditions);
            return connection.Query<T>(sb.ToString(), whereConditions, transaction, true, commandTimeout);
        }

        private static StringBuilder BuildGetListQuery<T>(object whereConditions)
        {
            var currenttype = typeof(T);
            var idProps = GetIdProperties(currenttype);
            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] property");

            var name = GetTableName(currenttype);

            var sb = new StringBuilder();
            var whereprops = GetAllProperties(whereConditions);
            sb.Append("Select ");
            //create a new empty instance of the type to get the base properties
            BuildSelect<T>(sb, GetScaffoldableProperties<T>());
            sb.AppendFormat(" from {0}", name);

            if (whereprops.Any())
            {
                sb.Append(" where ");
                BuildWhere<T>(sb, whereprops, whereConditions);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetList<{currenttype}>: {sb}");
            return sb;
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause and/or order by clause ex: "where name='bob'" or "where age>=@Age"</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Gets a list of entities with optional SQL where conditions</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildGetListQuery<T>(conditions);

            return connection.Query<T>(sb.ToString(), parameters, transaction, true, commandTimeout);
        }

        private static StringBuilder BuildGetListQuery<T>(string conditions)
        {
            var currenttype = typeof(T);
            var idProps = GetIdProperties(currenttype);
            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] property");

            var name = GetTableName(currenttype);

            var sb = new StringBuilder();
            sb.Append("Select ");
            //create a new empty instance of the type to get the base properties
            BuildSelect<T>(sb, GetScaffoldableProperties<T>());
            sb.AppendFormat(" from {0}", name);

            sb.Append(" " + conditions);

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetList<{currenttype}>: {sb}");
            return sb;
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Returns a list of all entities</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <returns>Gets a list of all entities</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection)
        {
            return connection.GetList<T>(new { });
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age" - not required </para>
        /// <para>orderby is a column or list of columns to order by ex: "lastname, age desc" - not required - default is by primary key</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="pageNumber"></param>
        /// <param name="rowsPerPage"></param>
        /// <param name="conditions"></param>
        /// <param name="orderby"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Gets a paged list of entities with optional exact match where conditions</returns>
        public static IEnumerable<T> GetListPaged<T>(this IDbConnection connection, int pageNumber, int rowsPerPage, string conditions, string orderby, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            string query = BuildGetListPagedQuery<T>(pageNumber, rowsPerPage, conditions, ref orderby);

            return connection.Query<T>(query, parameters, transaction, true, commandTimeout);
        }

        private static string BuildGetListPagedQuery<T>(int pageNumber, int rowsPerPage, string conditions, ref string orderby)
        {
            if (string.IsNullOrEmpty(_getPagedListSql))
                throw new Exception("GetListPage is not supported with the current SQL Dialect");

            if (pageNumber < 1)
                throw new Exception("Page must be greater than 0");

            var currenttype = typeof(T);
            var idProps = GetIdProperties(currenttype);
            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] property");

            var name = GetTableName(currenttype);
            var sb = new StringBuilder();
            var query = _getPagedListSql;
            if (string.IsNullOrEmpty(orderby))
            {
                orderby = GetColumnName(idProps.First());
            }

            //create a new empty instance of the type to get the base properties
            BuildSelect<T>(sb, GetScaffoldableProperties<T>());
            query = query.Replace("{SelectColumns}", sb.ToString())
                .Replace("{TableName}", name)
                .Replace("{PageNumber}", pageNumber.ToString())
                .Replace("{RowsPerPage}", rowsPerPage.ToString())
                .Replace("{OrderBy}", orderby)
                .Replace("{WhereClause}", conditions)
                .Replace("{Offset}", ((pageNumber - 1) * rowsPerPage).ToString());

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetListPaged<{currenttype}>: {query}");
            return query;
        }

        /// <summary>
        /// <para>Inserts a row into the database</para>
        /// <para>By default inserts into the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Insert filters out Id column and any columns with the [Key] attribute</para>
        /// <para>Properties marked with attribute [Editable(false)] and complex types are ignored</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns the ID (primary key) of the newly inserted record if it is identity using the int? type, otherwise null</para>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The ID (primary key) of the newly inserted record if it is identity using the int? type, otherwise null</returns>
        public static int? Insert<TEntity>(this IDbConnection connection, TEntity entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            return Insert<int?, TEntity>(connection, entityToInsert, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Inserts a row into the database, using ONLY the properties defined by TEntity</para>
        /// <para>By default inserts into the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Insert filters out Id column and any columns with the [Key] attribute</para>
        /// <para>Properties marked with attribute [Editable(false)] and complex types are ignored</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns the ID (primary key) of the newly inserted record if it is identity using the defined type, otherwise null</para>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="entityToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The ID (primary key) of the newly inserted record if it is identity using the defined type, otherwise null</returns>
        public static TKey Insert<TKey, TEntity>(this IDbConnection connection, TEntity entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            BuildInsertQuery<TKey, TEntity>(entityToInsert, out IReadOnlyList<PropertyInfo> idProps, out bool keyHasPredefinedValue, out Type keytype, out StringBuilder sb);

            var r = connection.Query(sb.ToString(), entityToInsert, transaction, true, commandTimeout);

            if (keytype == typeof(Guid) || keyHasPredefinedValue)
            {
                return (TKey)idProps.First().GetValue(entityToInsert, null);
            }
            return (TKey)r.First().id;
        }

        private static void BuildInsertQuery<TKey, TEntity>(TEntity entityToInsert, out IReadOnlyList<PropertyInfo> idProps, out bool keyHasPredefinedValue, out Type keytype, out StringBuilder sb)
        {
            idProps = GetIdProperties(entityToInsert);
            if (!idProps.Any())
                throw new ArgumentException("Insert<T> only supports an entity with a [Key] or Id property");

            keyHasPredefinedValue = false;
            var baseType = typeof(TKey);
            var underlyingType = Nullable.GetUnderlyingType(baseType);
            keytype = underlyingType ?? baseType;
            if (keytype != typeof(int) && keytype != typeof(uint) && keytype != typeof(long) && keytype != typeof(ulong) && keytype != typeof(short) && keytype != typeof(ushort) && keytype != typeof(Guid) && keytype != typeof(string))
            {
                throw new Exception("Invalid return type");
            }

            var name = GetTableName(entityToInsert);
            sb = new StringBuilder();
            sb.AppendFormat("insert into {0}", name);
            sb.Append(" (");
            BuildInsertParameters<TEntity>(sb);
            sb.Append(") ");
            sb.Append("values");
            sb.Append(" (");
            BuildInsertValues<TEntity>(sb);
            sb.Append(")");

            if (keytype == typeof(Guid))
            {
                var guidvalue = (Guid)idProps.First().GetValue(entityToInsert, null);
                if (guidvalue == Guid.Empty)
                {
                    var newguid = SequentialGuid();
                    idProps.First().SetValue(entityToInsert, newguid, null);
                }
                else
                {
                    keyHasPredefinedValue = true;
                }
                sb.Append(";select '" + idProps.First().GetValue(entityToInsert, null) + "' as id");
            }

            if ((keytype == typeof(int) || keytype == typeof(long)) && Convert.ToInt64(idProps.First().GetValue(entityToInsert, null)) == 0)
            {
                sb.Append(";" + _getIdentitySql);
            }
            else
            {
                keyHasPredefinedValue = true;
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Insert: {sb}");
        }

        /// <summary>
        /// <para>Updates a record or records in the database with only the properties of TEntity</para>
        /// <para>By default updates records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Updates records where the Id property and properties with the [Key] attribute match those in the database.</para>
        /// <para>Properties marked with attribute [Editable(false)] and complex types are ignored</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns number of rows affected</para>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="entityToUpdate"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of affected records</returns>
        public static int Update<TEntity>(this IDbConnection connection, TEntity entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildUpdateQuery(entityToUpdate);

            return connection.Execute(sb.ToString(), entityToUpdate, transaction, commandTimeout);
        }

        private static StringBuilder BuildUpdateQuery<TEntity>(TEntity entityToUpdate)
        {
            var idProps = GetIdProperties(entityToUpdate);

            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] or Id property");

            var name = GetTableName(entityToUpdate);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0}", name);

            sb.AppendFormat(" set ");
            BuildUpdateSet(entityToUpdate, sb);
            sb.Append(" where ");
            BuildWhere<TEntity>(sb, idProps, entityToUpdate);

            if (Debugger.IsAttached)
                Trace.WriteLine($"Update: {sb}");
            return sb;
        }

        /// <summary>
        /// <para>Deletes a record or records in the database that match the object passed in</para>
        /// <para>-By default deletes records in the table matching the class name</para>
        /// <para>Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>Returns the number of records affected</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToDelete"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int Delete<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteQuery(entityToDelete);

            return connection.Execute(sb.ToString(), entityToDelete, transaction, commandTimeout);
        }

        private static StringBuilder BuildDeleteQuery<T>(T entityToDelete)
        {
            var idProps = GetIdProperties(entityToDelete);

            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] or Id property");

            var name = GetTableName(entityToDelete);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0}", name);

            sb.Append(" where ");
            BuildWhere<T>(sb, idProps, entityToDelete);

            if (Debugger.IsAttached)
                Trace.WriteLine($"Delete: {sb}");
            return sb;
        }

        /// <summary>
        /// <para>Deletes a record or records in the database by ID</para>
        /// <para>By default deletes records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Deletes records where the Id property and properties with the [Key] attribute match those in the database</para>
        /// <para>The number of records affected</para>
        /// <para>Supports transaction and command timeout</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int Delete<T>(this IDbConnection connection, object id, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteQuery<T>(id, out DynamicParameters dynParms);

            return connection.Execute(sb.ToString(), dynParms, transaction, commandTimeout);
        }

        private static StringBuilder BuildDeleteQuery<T>(object id, out DynamicParameters dynParms)
        {
            var currenttype = typeof(T);
            var idProps = GetIdProperties(currenttype);


            if (!idProps.Any())
                throw new ArgumentException("Delete<T> only supports an entity with a [Key] or Id property");

            var name = GetTableName(currenttype);

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("Delete from {0} where ", name);

            bool hasAnyProperty = idProps
                .Select(p => sb.AppendFormat("{0} = @{1} and", GetColumnName(p), p.Name).Length)
                .Count() != 0;
            if (hasAnyProperty)
                sb.Remove(sb.Length - 4, 4);


            dynParms = new DynamicParameters();
            if (idProps.Count == 1)
                dynParms.Add("@" + idProps.First().Name, id);
            else
            {
                foreach (var prop in idProps)
                    dynParms.Add("@" + prop.Name, id.GetType().GetProperty(prop.Name).GetValue(id, null));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Delete<{currenttype}> {sb}");

            return sb;
        }

        /// <summary>
        /// <para>Deletes a list of records in the database</para>
        /// <para>By default deletes records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Deletes records where that match the where clause</para>
        /// <para>whereConditions is an anonymous type to filter the results ex: new {Category = 1, SubCategory=2}</para>
        /// <para>The number of records affected</para>
        /// <para>Supports transaction and command timeout</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int DeleteList<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteListQuery<T>(whereConditions);

            return connection.Execute(sb.ToString(), whereConditions, transaction, commandTimeout);
        }

        private static StringBuilder BuildDeleteListQuery<T>(object whereConditions)
        {
            var currenttype = typeof(T);
            var name = GetTableName(currenttype);

            var sb = new StringBuilder();
            var whereprops = GetAllProperties(whereConditions);
            sb.AppendFormat("Delete from {0}", name);
            if (whereprops.Any())
            {
                sb.Append(" where ");
                BuildWhere<T>(sb, whereprops);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteList<{currenttype}> {sb}");
            return sb;
        }

        private static StringBuilder BuildDeleteListQuery<T>(string conditions)
        {
            if (string.IsNullOrEmpty(conditions))
                throw new ArgumentException("DeleteList<T> requires a where clause");
            if (!conditions.ToLower().Contains("where"))
                throw new ArgumentException("DeleteList<T> requires a where clause and must contain the WHERE keyword");

            var currenttype = typeof(T);
            var name = GetTableName(currenttype);

            var sb = new StringBuilder();
            sb.AppendFormat("Delete from {0}", name);
            sb.Append(" " + conditions);

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteList<{currenttype}> {sb}");
            return sb;
        }

        /// <summary>
        /// <para>Deletes a list of records in the database</para>
        /// <para>By default deletes records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Deletes records where that match the where clause</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age"</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// <para>Supports transaction and command timeout</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static int DeleteList<T>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteListQuery<T>(conditions);

            return connection.Execute(sb.ToString(), parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Returns a number of records entity by a single id from table T</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age" - not required </para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Returns a count of records.</returns>
        public static int RecordCount<T>(this IDbConnection connection, string conditions = "", object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildRecordCountQuery<T>(conditions);
            return connection.ExecuteScalar<int>(sb.ToString(), parameters, transaction, commandTimeout);
        }

        private static StringBuilder BuildRecordCountQuery<T>(string conditions)
        {
            var currenttype = typeof(T);
            var name = GetTableName(currenttype);
            var sb = new StringBuilder();
            sb.Append("Select count(1)");
            sb.AppendFormat(" from {0}", name);
            sb.Append(" " + conditions);

            if (Debugger.IsAttached)
                Trace.WriteLine($"RecordCount<{currenttype}>: {sb}");
            return sb;
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Returns a number of records entity by a single id from table T</para>
        /// <para>Supports transaction and command timeout</para>
        /// <para>whereConditions is an anonymous type to filter the results ex: new {Category = 1, SubCategory=2}</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Returns a count of records.</returns>
        public static int RecordCount<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildRecordCountQuery<T>(whereConditions);

            return connection.ExecuteScalar<int>(sb.ToString(), whereConditions, transaction, commandTimeout);
        }

        private static StringBuilder BuildRecordCountQuery<T>(object whereConditions)
        {
            var currenttype = typeof(T);
            var name = GetTableName(currenttype);

            var sb = new StringBuilder();
            var whereprops = GetAllProperties(whereConditions);
            sb.Append("Select count(1)");
            sb.AppendFormat(" from {0}", name);
            if (whereprops.Any())
            {
                sb.Append(" where ");
                BuildWhere<T>(sb, whereprops);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"RecordCount<{currenttype}>: {sb}");
            return sb;
        }

        //build update statement based on list on an entity
        private static void BuildUpdateSet<T>(T entityToUpdate, StringBuilder masterSb)
        {
            AppendToStringBuilderCachedOrFuncValue(masterSb, $"{typeof(T).FullName}_BuildUpdateSet", NamedStringCache, sb =>
            {
                bool hasAnyProperty = GetUpdateableProperties(entityToUpdate)
                    .Select(p => sb.AppendFormat("{0} = @{1}, ", GetColumnName(p), p.Name).Length)
                    .Count() != 0;

                if(hasAnyProperty)
                    sb.Remove(sb.Length - 2, 2);

            });
        }

        //build select clause based on list of properties skipping ones with the IgnoreSelect and NotMapped attribute
        private static void BuildSelect<T>(StringBuilder masterSb, IReadOnlyList<PropertyInfo> props)
        {
            AppendToStringBuilderCachedOrFuncValue(masterSb, $"{typeof(T).FullName}_BuildSelect", NamedStringCache, sb => {
                if (props
                    .Where(property =>
                        !property.GetCustomAttributes(typeof(IgnoreSelectAttribute), true).Any()
                        && ! property.GetCustomAttributes(typeof(NotMappedAttribute), true).Any()
                    )
                    .Select(property =>
                        property.GetCustomAttributes(typeof(ColumnAttribute), true).Any()
                            //if there is a custom column name add an "as customcolumnname" to the item so it maps properly
                            ? sb.Append($"{GetColumnName(property)} as {Encapsulate(property.Name)},").Length
                            : sb.Append($"{GetColumnName(property)},").Length
                    )
                    .Count() != 0)
                    sb.Remove(sb.Length - 1, 1);
            });
        }

        private static void BuildWhere<TEntity>(StringBuilder sb, IEnumerable<PropertyInfo> idProps, object whereConditions = null)
        {
            var propertyInfos = idProps;
            var sourceProperties = GetScaffoldableProperties<TEntity>();

            foreach (var prop in propertyInfos)
            {
                var useIsNull = false;
                var propertyToUse = prop;
                //match up generic properties to source entity properties to allow fetching of the column attribute
                //the anonymous object used for search doesn't have the custom attributes attached to them so this allows us to build the correct where clause
                //by converting the model type to the database column name via the column attribute
                foreach (var sourceProperty in sourceProperties)
                {
                    if (sourceProperty.Name == propertyToUse.Name)
                    {
                        var propertyValue = propertyToUse.GetValue(whereConditions, null);
                        if (whereConditions != null && propertyToUse.CanRead && (propertyValue == null || propertyValue == DBNull.Value))
                        {
                            useIsNull = true;
                        }
                        propertyToUse = sourceProperty;
                        break;
                    }
                }
                sb.AppendFormat(
                    useIsNull ? "{0} is null" : "{0} = @{1}",
                    GetColumnName(propertyToUse),
                    propertyToUse.Name);

                    sb.AppendFormat(" and ");
            }
            if (propertyInfos.Count() != 0)
                sb.Remove(sb.Length - 5, 5);
        }

        //build insert values which include all properties in the class that are:
        //Not named Id
        //Not marked with the Editable(false) attribute
        //Not marked with the [Key] attribute (without required attribute)
        //Not marked with [IgnoreInsert]
        //Not marked with [NotMapped]
        private static void BuildInsertValues<T>(StringBuilder masterSb)
        {
            AppendToStringBuilderCachedOrFuncValue(masterSb, $"{typeof(T).FullName}_BuildInsertValues", NamedStringCache, sb =>
            {
                var props = GetScaffoldableProperties<T>();
                foreach (var property in props)
                {
                    if (property.PropertyType != typeof(Guid) && property.PropertyType != typeof(string)
                          && property.GetCustomAttributes(typeof(KeyAttribute), true).Any()
                          && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name))
                        continue;
                    if (property.GetCustomAttributes(typeof(IgnoreInsertAttribute), true).Any()
                        || property.GetCustomAttributes(typeof(NotMappedAttribute), true).Any()
                        || (property.GetCustomAttributes(typeof(ReadOnlyAttribute), true).Any() && IsReadOnly(property))
                    ) continue;

                    if (property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase) && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name) && property.PropertyType != typeof(Guid)) continue;

                    sb.AppendFormat("@{0}", property.Name);
                    sb.Append(", ");
                }
                if (props.Count()!=0)
                    sb.Remove(sb.Length - 2, 2);
            });

        }

        //build insert parameters which include all properties in the class that are not:
        //marked with the Editable(false) attribute
        //marked with the [Key] attribute
        //marked with [IgnoreInsert]
        //named Id
        //marked with [NotMapped]
        private static void BuildInsertParameters<T>(StringBuilder masterSb)
        {
            AppendToStringBuilderCachedOrFuncValue(masterSb, $"{typeof(T).FullName}_BuildInsertParameters", NamedStringCache, sb =>
            {
                var props = GetScaffoldableProperties<T>();

                foreach (var property in props)
                {
                    if (property.PropertyType != typeof(Guid) && property.PropertyType != typeof(string)
                          && property.GetCustomAttributes(typeof(KeyAttribute),true).Any()
                          && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name))
                        continue;
                    if (property.GetCustomAttributes(typeof(IgnoreInsertAttribute), true).Any()
                        || property.GetCustomAttributes(typeof(NotMappedAttribute), true).Any()
                        || (property.GetCustomAttributes(typeof(ReadOnlyAttribute), true).Any() && IsReadOnly(property))
                    ) continue;

                    if (property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase) && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name) && property.PropertyType != typeof(Guid)) continue;

                    sb.Append(GetColumnName(property));
                    sb.Append(", ");
                }
                if (props.Count()!=0)
                    sb.Remove(sb.Length - 2, 2);
            });
            
        }

        //Get all properties in an entity
        private static IReadOnlyList<PropertyInfo> GetAllProperties<T>(T entity) where T : class
            => ReturnCachedOrFuncValue($"{typeof(T).FullName}{entity?.GetType()?.FullName}", AllPropertiesCache, () => {
                if (entity == null)
                    return new PropertyInfo[0];
                return entity.GetType().GetProperties();
            });
        

        //Get all properties that are not decorated with the Editable(false) attribute
        private static IReadOnlyList<PropertyInfo> GetScaffoldableProperties<T>()
            => ReturnCachedOrFuncValue(typeof(T).FullName, ScaffoldablePropertiesCache, () => 
                typeof(T).GetProperties()
                    .Where(p => p.GetCustomAttributes(typeof(EditableAttribute), true).Any(attr => !IsEditable(p)) == false)
                    .Where(p => p.PropertyType.IsSimpleType() || IsEditable(p)).ToList()
            );

        //Determine if the Attribute has an AllowEdit key and return its boolean state
        //fake the funk and try to mimick EditableAttribute in System.ComponentModel.DataAnnotations 
        //This allows use of the DataAnnotations property in the model and have the SimpleCRUD engine just figure it out without a reference
        private static bool IsEditable(PropertyInfo pi)
        {
            dynamic write = pi.GetCustomAttributes(typeof(EditableAttribute),false).FirstOrDefault();
            if (write != null)
            {
                return write.AllowEdit;
            }
            return false;
        }


        //Determine if the Attribute has an IsReadOnly key and return its boolean state
        //fake the funk and try to mimick ReadOnlyAttribute in System.ComponentModel 
        //This allows use of the DataAnnotations property in the model and have the SimpleCRUD engine just figure it out without a reference
        private static bool IsReadOnly(PropertyInfo pi)
        {
            dynamic write = pi.GetCustomAttributes(typeof(ReadOnlyAttribute), false).FirstOrDefault();
            if (write != null)
            {
                return write.IsReadOnly;
            }
            return false;
        }

        //Get all properties that are:
        //Not named Id
        //Not marked with the Key attribute
        //Not marked ReadOnly
        //Not marked IgnoreInsert
        //Not marked NotMapped
        private static IReadOnlyList<PropertyInfo> GetUpdateableProperties<T>(T entity)
            => ReturnCachedOrFuncValue($"{typeof(T).FullName}{entity?.GetType()?.FullName}", UpdateablePropertiesCache, () => GetUpdateablePropertiesValue(entity));
        private static IReadOnlyList<PropertyInfo> GetUpdateablePropertiesValue<T>(T entity)
        {
            return GetScaffoldableProperties<T>()
            //remove ones with ID
            .Where(p => !p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
            //remove ones with key attribute
            .Where(p => p.GetCustomAttributes(typeof(KeyAttribute),true).Any() == false)
            //remove ones that are readonly
            .Where(p => p.GetCustomAttributes(typeof(ReadOnlyAttribute),true).Any(attr => IsReadOnly(p)) == false)
            //remove ones with IgnoreUpdate attribute
            .Where(p => p.GetCustomAttributes(typeof(IgnoreUpdateAttribute), true).Any() == false)
            //remove ones that are not mapped
            .Where(p => p.GetCustomAttributes(typeof(NotMappedAttribute), true).Any() == false).ToList();

        }

        //Get all properties that are named Id or have the Key attribute
        //For Inserts and updates we have a whole entity so this method is used
        private static IReadOnlyList<PropertyInfo> GetIdProperties(object entity)
            => ReturnCachedOrFuncValue(entity?.GetType()?.FullName, IdPropertiesCache, () =>
                GetIdProperties(entity.GetType())
            );
        
        //Get all properties that are named Id or have the Key attribute
        //For Get(id) and Delete(id) we don't have an entity, just the type so this method is used
        private static IReadOnlyList<PropertyInfo> GetIdProperties(Type type)
            => ReturnCachedOrFuncValue(type.FullName, IdPropertiesCache, () => {
                var tp = type.GetProperties().Where(p => p.GetCustomAttributes(typeof(KeyAttribute), true).Any()).ToList();
                return tp.Any() ? tp : type.GetProperties().Where(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)).ToList();
            });
        

        //Gets the table name for this entity
        //For Inserts and updates we have a whole entity so this method is used
        //Uses class name by default and overrides if the class has a Table attribute
        private static string GetTableName(object entity)
            => GetTableName(entity.GetType());
        

        //Gets the table name for this type
        //For Get(id) and Delete(id) we don't have an entity, just the type so this method is used
        //Use dynamic type to be able to handle both our Table-attribute and the DataAnnotation
        //Uses class name by default and overrides if the class has a Table attribute
        private static string GetTableName(Type type)
        {
            if (TableNames.TryGetValue(type, out string tableName))
                return tableName;

            tableName = _tableNameResolver.ResolveTableName(type);

            TableNames.AddOrUpdate(type, tableName, (t, v) => tableName);

            return tableName;
        }

        private static string GetColumnName(PropertyInfo propertyInfo)
        {
            string key = $"{propertyInfo.DeclaringType}.{propertyInfo.Name}";
            if (ColumnNames.TryGetValue(key, out string columnName))
                return columnName;

            columnName = _columnNameResolver.ResolveColumnName(propertyInfo);

            ColumnNames.AddOrUpdate(key, columnName, (t, v) => columnName);

            return columnName;
        }

        private static string Encapsulate(string databaseword)
        {
            return string.Format(_encapsulation, databaseword);
        }
        /// <summary>
        /// Generates a guid based on the current date/time
        /// http://stackoverflow.com/questions/1752004/sequential-guid-generator-c-sharp
        /// </summary>
        /// <returns></returns>
        public static Guid SequentialGuid()
        {
            var tempGuid = Guid.NewGuid();
            var bytes = tempGuid.ToByteArray();
            var time = DateTime.Now;
            bytes[3] = (byte)time.Year;
            bytes[2] = (byte)time.Month;
            bytes[1] = (byte)time.Day;
            bytes[0] = (byte)time.Hour;
            bytes[5] = (byte)time.Minute;
            bytes[4] = (byte)time.Second;
            return new Guid(bytes);
        }

        /// <summary>
        /// Database server dialects
        /// </summary>
        public enum Dialect
        {
            SQLServer,
            PostgreSQL,
            SQLite,
            MySQL,
        }

        public interface ITableNameResolver
        {
            string ResolveTableName(Type type);
        }

        public interface IColumnNameResolver
        {
            string ResolveColumnName(PropertyInfo propertyInfo);
        }

        public class TableNameResolver : ITableNameResolver
        {
            public virtual string ResolveTableName(Type type)
            {
                var tableName = Encapsulate(type.Name);

                var tableattr = type.GetCustomAttributes(typeof(TableAttribute),true).SingleOrDefault() as dynamic;
                if (tableattr != null)
                {
                    tableName = Encapsulate(tableattr.Name);
                    try
                    {
                        if (!String.IsNullOrEmpty(tableattr.Schema))
                        {
                            string schemaName = Encapsulate(tableattr.Schema);
                            tableName = $"{schemaName}.{tableName}";
                        }
                    }
                    catch (RuntimeBinderException)
                    {
                        //Schema doesn't exist on this attribute.
                    }
                }

                return tableName;
            }
        }

        public class ColumnNameResolver : IColumnNameResolver
        {
            public virtual string ResolveColumnName(PropertyInfo propertyInfo)
            {
                var columnName = Encapsulate(propertyInfo.Name);

                var columnattr = propertyInfo.GetCustomAttributes(typeof(ColumnAttribute),true).SingleOrDefault() as dynamic;
                if (columnattr != null)
                {
                    columnName = Encapsulate(columnattr.Name);
                    if (Debugger.IsAttached)
                        Trace.WriteLine($"Column name for type overridden from {propertyInfo.Name} to {columnName}");
                }
                return columnName;
            }
        }
    }

    /// <summary>
    /// Optional Table attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the table name of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        /// <summary>
        /// Optional Table attribute.
        /// </summary>
        /// <param name="tableName"></param>
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }
        /// <summary>
        /// Name of the table
        /// </summary>
        public string Name { get; private set; }
        /// <summary>
        /// Name of the schema
        /// </summary>
        public string Schema { get; set; }
    }

    /// <summary>
    /// Optional Column attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the table name of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        /// <summary>
        /// Optional Column attribute.
        /// </summary>
        /// <param name="columnName"></param>
        public ColumnAttribute(string columnName)
        {
            Name = columnName;
        }
        /// <summary>
        /// Name of the column
        /// </summary>
        public string Name { get; private set; }
    }

    /// <summary>
    /// Optional Key attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the Primary Key of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional NotMapped attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify that the property is not mapped
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class NotMappedAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional Key attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify a required property of a poco
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class RequiredAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional Editable attribute.
    /// You can use the System.ComponentModel.DataAnnotations version in its place to specify the properties that are editable
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class EditableAttribute : Attribute
    {
        /// <summary>
        /// Optional Editable attribute.
        /// </summary>
        /// <param name="iseditable"></param>
        public EditableAttribute(bool iseditable)
        {
            AllowEdit = iseditable;
        }
        /// <summary>
        /// Does this property persist to the database?
        /// </summary>
        public bool AllowEdit { get; private set; }
    }

    /// <summary>
    /// Optional Readonly attribute.
    /// You can use the System.ComponentModel version in its place to specify the properties that are editable
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ReadOnlyAttribute : Attribute
    {
        /// <summary>
        /// Optional ReadOnly attribute.
        /// </summary>
        /// <param name="isReadOnly"></param>
        public ReadOnlyAttribute(bool isReadOnly)
        {
            IsReadOnly = isReadOnly;
        }
        /// <summary>
        /// Does this property persist to the database?
        /// </summary>
        public bool IsReadOnly { get; private set; }
    }

    /// <summary>
    /// Optional IgnoreSelect attribute.
    /// Custom for Dapper.SimpleCRUD to exclude a property from Select methods
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreSelectAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional IgnoreInsert attribute.
    /// Custom for Dapper.SimpleCRUD to exclude a property from Insert methods
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreInsertAttribute : Attribute
    {
    }

    /// <summary>
    /// Optional IgnoreUpdate attribute.
    /// Custom for Dapper.SimpleCRUD to exclude a property from Update methods
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreUpdateAttribute : Attribute
    {
    }

}

internal static class TypeExtension
{
    //You can't insert or update complex types. Lets filter them out.
    public static bool IsSimpleType(this Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type);
        type = underlyingType ?? type;
        var simpleTypes = new List<Type>
                               {
                                   typeof(byte),
                                   typeof(sbyte),
                                   typeof(short),
                                   typeof(ushort),
                                   typeof(int),
                                   typeof(uint),
                                   typeof(long),
                                   typeof(ulong),
                                   typeof(float),
                                   typeof(double),
                                   typeof(decimal),
                                   typeof(bool),
                                   typeof(string),
                                   typeof(char),
                                   typeof(Guid),
                                   typeof(DateTime),
                                   typeof(DateTimeOffset),
                                   typeof(byte[])
                               };
        return simpleTypes.Contains(type) || type.IsEnum;
    }
}

internal static class LinqExtensions
{
    public static IEnumerable<TSource> Except<TSource>(this IEnumerable<TSource> first, IEnumerable<TSource> second, Func<TSource, TSource, bool> comparer)
    {
        return first.Where(x => second.Count(y => comparer(x, y)) == 0);
    }

    public static IEnumerable<TSource> Intersect<TSource>(this IEnumerable<TSource> first, IEnumerable<TSource> second, Func<TSource, TSource, bool> comparer)
    {
        return first.Where(x => second.Count(y => comparer(x, y)) == 1);
    }
}