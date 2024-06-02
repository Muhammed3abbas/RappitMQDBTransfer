using Dapper;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQDBTransfer
{
    public class DataRetriever
    {
        private readonly string _connectionString;

        public DataRetriever(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<string> GetDataAsync()
        {
            StringBuilder sb = new StringBuilder();

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                string sql = "SELECT FirstName, LastName FROM Employees";
                var employees = await connection.QueryAsync<Employee>(sql);

                foreach (var employee in employees)
                {
                    sb.AppendLine($"Employee|{employee.FirstName},{employee.LastName}");
                }
            }

            return sb.ToString();
        }
    }


}
