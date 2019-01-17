using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper
{
    /// <summary>
    /// Main class for Dapper.SimpleCRUD extensions
    /// </summary>
    public static partial class SimpleCRUD
    {
        /// <summary>
        /// <para>By default queries the table matching the class name asynchronously </para>
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
        public static async Task<T> GetAsync<T>(this IDbConnection connection, object id, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildGetQuery<T>(id, out DynamicParameters dynParms);
            var query = await connection.QueryAsync<T>(sb.ToString(), dynParms, transaction, commandTimeout);
            return query.FirstOrDefault();
        }

        /// <summary>
        /// <para>By default queries the table matching the class name asynchronously</para>
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
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildGetListQuery<T>(whereConditions);
            return connection.QueryAsync<T>(sb.ToString(), whereConditions, transaction, commandTimeout);
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
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildGetListQuery<T>(conditions);
            return connection.QueryAsync<T>(sb.ToString(), parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>By default queries the table matching the class name asynchronously</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Returns a list of all entities</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <returns>Gets a list of all entities</returns>
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection)
        {
            return connection.GetListAsync<T>(new { });
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age" - not required </para>
        /// <para>orderby is a column or list of columns to order by ex: "lastname, age desc" - not required - default is by primary key</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
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
        /// <returns>Gets a list of entities with optional exact match where conditions</returns>
        public static Task<IEnumerable<T>> GetListPagedAsync<T>(this IDbConnection connection, int pageNumber, int rowsPerPage, string conditions, string orderby, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            string query = BuildGetListPagedQuery<T>(pageNumber, rowsPerPage, conditions, ref orderby);

            return connection.QueryAsync<T>(query, parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Inserts a row into the database asynchronously</para>
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
        public static Task<int?> InsertAsync<TEntity>(this IDbConnection connection, TEntity entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            return InsertAsync<int?, TEntity>(connection, entityToInsert, transaction, commandTimeout);
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
        public static async Task<TKey> InsertAsync<TKey, TEntity>(this IDbConnection connection, TEntity entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            BuildInsertQuery<TKey, TEntity>(entityToInsert, out IReadOnlyList<PropertyInfo> idProps, out bool keyHasPredefinedValue, out Type keytype, out StringBuilder sb);

            if (keytype == typeof(Guid) || keyHasPredefinedValue)
            {
                await connection.ExecuteAsync(sb.ToString(), entityToInsert, transaction, commandTimeout);
                return (TKey)idProps.First().GetValue(entityToInsert, null);
            }
            var r = await connection.QueryAsync(sb.ToString(), entityToInsert, transaction, commandTimeout);
            return (TKey)r.First().id;
        }
        
        /// <summary>
        ///  <para>Updates a record or records in the database asynchronously</para>
        ///  <para>By default updates records in the table matching the class name</para>
        ///  <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        ///  <para>Updates records where the Id property and properties with the [Key] attribute match those in the database.</para>
        ///  <para>Properties marked with attribute [Editable(false)] and complex types are ignored</para>
        ///  <para>Supports transaction and command timeout</para>
        ///  <para>Returns number of rows affected</para>
        ///  </summary>
        ///  <param name="connection"></param>
        ///  <param name="entityToUpdate"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of affected records</returns>
        public static Task<int> UpdateAsync<TEntity>(this IDbConnection connection, TEntity entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null, System.Threading.CancellationToken? token = null)
        {
            StringBuilder sb = BuildUpdateQuery(entityToUpdate);

            System.Threading.CancellationToken cancelToken = token ?? default(System.Threading.CancellationToken);
            return connection.ExecuteAsync(new CommandDefinition(sb.ToString(), entityToUpdate, transaction, commandTimeout, cancellationToken: cancelToken));
        }

        /// <summary>
        /// <para>Deletes a record or records in the database that match the object passed in asynchronously</para>
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
        public static Task<int> DeleteAsync<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteQuery(entityToDelete);

            return connection.ExecuteAsync(sb.ToString(), entityToDelete, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes a record or records in the database by ID asynchronously</para>
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
        public static Task<int> DeleteAsync<T>(this IDbConnection connection, object id, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteQuery<T>(id, out DynamicParameters dynParms);
            return connection.ExecuteAsync(sb.ToString(), dynParms, transaction, commandTimeout);
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
        public static Task<int> DeleteListAsync<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteListQuery<T>(whereConditions);
            return connection.ExecuteAsync(sb.ToString(), whereConditions, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes a list of records in the database</para>
        /// <para>By default deletes records in the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>Deletes records where that match the where clause</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age"</para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>The number of records affected</returns>
        public static Task<int> DeleteListAsync<T>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildDeleteListQuery<T>(conditions);
            return connection.ExecuteAsync(sb.ToString(), parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" or "where age>=@Age" - not required </para>
        /// <para>parameters is an anonymous type to pass in named parameter values: new { Age = 15 }</para>   
        /// <para>Supports transaction and command timeout</para>
        /// /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>Returns a count of records.</returns>
        public static Task<int> RecordCountAsync<T>(this IDbConnection connection, string conditions = "", object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildRecordCountQuery<T>(conditions);

            return connection.ExecuteScalarAsync<int>(sb.ToString(), parameters, transaction, commandTimeout);
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
        public static Task<int> RecordCountAsync<T>(this IDbConnection connection, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            StringBuilder sb = BuildRecordCountQuery<T>(whereConditions);

            return connection.ExecuteScalarAsync<int>(sb.ToString(), whereConditions, transaction, commandTimeout);
        }
    }
}

