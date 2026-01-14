using Snowflake.Data.Client;
using SnowflakeAPI.Models;
using System.Data;

namespace SnowflakeAPI.Services
{
    public class SnowflakeService
    {
        private readonly string _connectionString;

        public SnowflakeService(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("Snowflake") 
                ?? throw new InvalidOperationException("Connection string not found");
        }

        public async Task<List<User>> GetUsersAsync()
        {
            var users = new List<User>();

            using var connection = new SnowflakeDbConnection();
            connection.ConnectionString = _connectionString;
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT id, name, email, created_at FROM users";

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                users.Add(new User
                {
                    Id = reader.GetInt32(0),
                    Name = reader.GetString(1),
                    Email = reader.GetString(2),
                    CreatedAt = reader.IsDBNull(3) ? DateTime.Now : ParseSnowflakeTimestamp(reader.GetValue(3))
                });
            }

            return users;
        }

        public async Task<User?> GetUserByIdAsync(int id)
        {
            using var connection = new SnowflakeDbConnection();
            connection.ConnectionString = _connectionString;
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT id, name, email, created_at FROM users WHERE id = ?";
            command.Parameters.Add(new SnowflakeDbParameter { Value = id });

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return new User
                {
                    Id = reader.GetInt32(0),
                    Name = reader.GetString(1),
                    Email = reader.GetString(2),
                    CreatedAt = reader.IsDBNull(3) ? DateTime.Now : ParseSnowflakeTimestamp(reader.GetValue(3))
                };
            }

            return null;
        }
        public async Task<bool> DeleteUserAsync(int id)
        {
            using var connection = new SnowflakeDbConnection();
            connection.ConnectionString = _connectionString;
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM my_app_db.main_schema.users WHERE id = :1";
            
            var param = new SnowflakeDbParameter
            {
                ParameterName = "1",
                Value = id,
                DbType = System.Data.DbType.Int32
            };
    
            command.Parameters.Add(param);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            return rowsAffected > 0;
        }
        public async Task<bool> UpdateUserAsync(int id, User user)
{
    using var connection = new SnowflakeDbConnection();
    connection.ConnectionString = _connectionString;
    await connection.OpenAsync();

    using var command = connection.CreateCommand();
    command.CommandText = "UPDATE my_app_db.main_schema.users SET name = :1, email = :2 WHERE id = :3";
    
    var param1 = new SnowflakeDbParameter
    {
        ParameterName = "1",
        Value = user.Name,
        DbType = System.Data.DbType.String
    };
    
    var param2 = new SnowflakeDbParameter
    {
        ParameterName = "2",
        Value = user.Email,
        DbType = System.Data.DbType.String
    };
    
    var param3 = new SnowflakeDbParameter
    {
        ParameterName = "3",
        Value = id,
        DbType = System.Data.DbType.Int32
    };
    
    command.Parameters.Add(param1);
    command.Parameters.Add(param2);
    command.Parameters.Add(param3);

    var rowsAffected = await command.ExecuteNonQueryAsync();
    return rowsAffected > 0;
}

       public async Task<int> CreateUserAsync(User user)
{
    using var connection = new SnowflakeDbConnection();
    connection.ConnectionString = _connectionString;
    await connection.OpenAsync();

    using var command = connection.CreateCommand();
    command.CommandText = "INSERT INTO my_app_db.main_schema.users (name, email) VALUES (:1, :2)";
    
    var param1 = new SnowflakeDbParameter
    {
        ParameterName = "1",
        Value = user.Name,
        DbType = System.Data.DbType.String
    };
    
    var param2 = new SnowflakeDbParameter
    {
        ParameterName = "2",
        Value = user.Email,
        DbType = System.Data.DbType.String
    };
    
    command.Parameters.Add(param1);
    command.Parameters.Add(param2);

    return await command.ExecuteNonQueryAsync();
}
        private DateTime ParseSnowflakeTimestamp(object value)
        {
            if (value is DateTime dt)
            {
                return dt;
            }
            
            if (value is string strValue && DateTime.TryParse(strValue, out DateTime parsedDate))
            {
                return parsedDate;
            }

            // Handle decimal/numeric timestamp (seconds since epoch)
            if (decimal.TryParse(value.ToString(), out decimal timestamp))
            {
                return DateTimeOffset.FromUnixTimeSeconds((long)timestamp).DateTime;
            }

            return DateTime.Now;
        }
    }
}