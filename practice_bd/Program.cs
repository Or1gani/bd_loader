using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Dapper;
using Npgsql;
using Newtonsoft.Json;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace practice_bd
{
    internal class Program
    {
        private static string connectionString = "Host=localhost;Username=postgres;Password=123;Database=pkp";

        static void Main(string[] args)
        {

            Console.WriteLine("Введите строку подключения: ");
            while (true)
            {
                Console.WriteLine("Host:");
                string host = Console.ReadLine();
                Console.WriteLine("Username:");
                string username = Console.ReadLine();
                Console.WriteLine("Password:");
                string password = Console.ReadLine();
                Console.WriteLine("Database:");
                string database = Console.ReadLine();
                Console.WriteLine("Вы уверены в строке подключения? ( Y / N )");
                if (Console.ReadLine() == "Y" || Console.ReadLine() == "y")
                {
                    connectionString = $"Host={host};Username={username};Password={password};Database={database}";
                    Console.Clear();
                    break;
                }
                else if (Console.ReadLine() == "N" || Console.ReadLine() == "n")
                {
                    connectionString = string.Empty;
                    Console.WriteLine("Введите строку подключения: ");

                }
            }

            Console.WriteLine("Введите путь сохранения файлов:");
            string savePath = Console.ReadLine();
            while (true)
            {
                Console.WriteLine("Введите команду (например, 'mater_tsennost' или 'mater_tsennost -f путь_к_csv_файлу.csv'):");
                string command = Console.ReadLine();

                if (command.ToLower() == "exit")
                {
                    break;
                }

                try
                {
                    ProcessCommand(command, savePath);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка: {ex.Message}");
                }
            }
        }

        public static void ProcessCommand(string command, string savePath)
        {
            string[] parts = command.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length == 0)
            {
                Console.WriteLine("Не указана команда.");
                return;
            }

            string tableName = parts[0];
            List<FilterCondition> filters = null;

            if (parts.Length > 2 && parts[1] == "-f")
            {
                string csvFilePath = parts[2];
                filters = ReadFiltersFromCsv(csvFilePath);
            }

            ExportDataToJsonWithFilters(tableName, savePath, filters);
        }

        public static void ExportDataToJsonWithFilters(string tableName, string savePath, List<FilterCondition> filters = null)
        {
            using (IDbConnection db = new NpgsqlConnection(connectionString))
            {
                // Получение данных основной таблицы
                var mainData = GetDataFromTable(db, tableName, filters);
                if (mainData.Rows.Count == 0)
                {
                    Console.WriteLine($"Нет данных для экспорта из таблицы '{tableName}' с указанными фильтрами.");
                    return;
                }

                var allData = new Dictionary<string, DataTable>
                {
                    { tableName, mainData }
                };

                // Получение и фильтрация данных из связанных таблиц
                var relatedTables = GetRelatedTables(tableName, db);
                foreach (var relatedTable in relatedTables)
                {
                    var relatedData = GetFilteredRelatedData(db, relatedTable, mainData, tableName);
                    allData[relatedTable] = relatedData;
                }

                // Формирование имени файла
                string fileName = tableName;
                if (filters != null && filters.Any())
                {
                    fileName += "_f";
                }
                if (relatedTables.Any())
                {
                    fileName += "_wr";
                }
                fileName += ".json";

                // Запись всех данных в JSON
                string filePath = Path.Combine(savePath, fileName);
                WriteAllDataToJson(allData, filePath);
                Console.WriteLine($"Данные экспортированы в {filePath}");
            }
        }

        private static DataTable GetDataFromTable(IDbConnection db, string tableName, List<FilterCondition> filters = null)
        {
            string query = $"SELECT * FROM {tableName}";
            var data = db.Query(query).ToList();
            var dataTable = ToDataTable(data);

            // Применение фильтров, если они заданы
            if (filters != null && filters.Any())
            {
                ApplyFilters(dataTable, filters);
            }

            return dataTable;
        }

        private static DataTable ToDataTable(IEnumerable<dynamic> items)
        {
            var dataTable = new DataTable();
            foreach (var item in items)
            {
                if (dataTable.Columns.Count == 0)
                {
                    foreach (var prop in ((IDictionary<string, object>)item).Keys)
                    {
                        dataTable.Columns.Add(prop);
                    }
                }
                var row = dataTable.NewRow();
                foreach (var prop in ((IDictionary<string, object>)item))
                {
                    row[prop.Key] = prop.Value;
                }
                dataTable.Rows.Add(row);
            }
            return dataTable;
        }

        private static void WriteAllDataToJson(Dictionary<string, DataTable> allData, string filePath)
        {
            var json = JsonConvert.SerializeObject(allData, Formatting.Indented);
            File.WriteAllText(filePath, json);
        }

        private static List<string> GetRelatedTables(string tableName, IDbConnection db)
        {
            string query = @"
                SELECT DISTINCT
                    kcu2.table_name AS related_table
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu1
                        ON tc.constraint_name = kcu1.constraint_name
                    JOIN information_schema.referential_constraints AS rc
                        ON tc.constraint_name = rc.constraint_name
                    JOIN information_schema.key_column_usage AS kcu2
                        ON rc.unique_constraint_name = kcu2.constraint_name
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND kcu1.table_name = @TableName";

            var relatedTables = db.Query<string>(query, new { TableName = tableName }).ToList();
            return relatedTables;
        }

        private static DataTable GetFilteredRelatedData(IDbConnection db, string relatedTable, DataTable mainData, string mainTableName)
        {
            var relatedData = GetDataFromTable(db, relatedTable);
            var relatedDataTable = new DataTable();

            foreach (DataColumn column in relatedData.Columns)
            {
                relatedDataTable.Columns.Add(column.ColumnName, column.DataType);
            }

            foreach (DataRow mainRow in mainData.Rows)
            {
                foreach (DataRow relatedRow in relatedData.Rows)
                {
                    if (IsRelated(mainRow, relatedRow, mainTableName, relatedTable, db))
                    {
                        relatedDataTable.ImportRow(relatedRow);
                    }
                }
            }

            return relatedDataTable;
        }

        private static bool IsRelated(DataRow mainRow, DataRow relatedRow, string mainTableName, string relatedTable, IDbConnection db)
        {
            string query = @"
                SELECT
                    kcu1.column_name AS main_column,
                    kcu2.column_name AS related_column
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu1
                        ON tc.constraint_name = kcu1.constraint_name
                    JOIN information_schema.referential_constraints AS rc
                        ON tc.constraint_name = rc.constraint_name
                    JOIN information_schema.key_column_usage AS kcu2
                        ON rc.unique_constraint_name = kcu2.constraint_name
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND kcu1.table_name = @MainTableName
                    AND kcu2.table_name = @RelatedTable";

            var columns = db.Query(query, new { MainTableName = mainTableName, RelatedTable = relatedTable }).ToList();

            foreach (var column in columns)
            {
                if (mainRow[column.main_column].ToString() == relatedRow[column.related_column].ToString())
                {
                    return true;
                }
            }

            return false;
        }

        private static List<FilterCondition> ReadFiltersFromCsv(string csvFilePath)
        {
            var filters = new List<FilterCondition>();

            using (var reader = new StreamReader(csvFilePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();

                while (csv.Read())
                {
                    var filterColumn = csv.GetField<string>("filter_column");
                    var filterOperator = csv.GetField<string>("filter_operator");
                    var filterValue = csv.GetField<string>("filter_value");

                    filters.Add(new FilterCondition
                    {
                        ColumnName = filterColumn,
                        Operator = filterOperator,
                        Value = filterValue
                    });
                }
            }

            return filters;
        }

        private static void ApplyFilters(DataTable dataTable, List<FilterCondition> filters)
        {
            foreach (var filter in filters)
            {
                for (int i = dataTable.Rows.Count - 1; i >= 0; i--)
                {
                    var cellValue = dataTable.Rows[i][filter.ColumnName];

                    if (!EvaluateCondition(cellValue, filter.Operator, filter.Value))
                    {
                        dataTable.Rows.RemoveAt(i);
                    }
                }
            }
        }

        private static bool EvaluateCondition(object cellValue, string filterOperator, string filterValue)
        {
            switch (filterOperator)
            {
                case "=":
                    return cellValue.ToString() == filterValue;
                case ">":
                    return Convert.ToDouble(cellValue) > Convert.ToDouble(filterValue);
                case "<":
                    return Convert.ToDouble(cellValue) < Convert.ToDouble(filterValue);
                case ">=":
                    return Convert.ToDouble(cellValue) >= Convert.ToDouble(filterValue);
                case "<=":
                    return Convert.ToDouble(cellValue) <= Convert.ToDouble(filterValue);
                case "!=":
                    return cellValue.ToString() != filterValue;
                default:
                    throw new ArgumentException($"Неизвестный оператор фильтра: {filterOperator}");
            }
        }

        public class FilterCondition
        {
            public string ColumnName { get; set; }
            public string Operator { get; set; }
            public string Value { get; set; }
        }
    }
}
