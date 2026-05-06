using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace OracleBulkCopy;

public sealed class OracleBulkCopy : IDisposable, IAsyncDisposable
{
	// https://github.com/Microsoft/referencesource/blob/master/System.Data/System/Data/SqlClient/SqlBulkCopy.cs
	// https://stackoverflow.com/questions/47942691/how-to-make-a-bulk-insert-using-oracle-managed-data-acess-c-sharp
	// https://github.com/DigitalPlatform/dp2/blob/master/DigitalPlatform.rms.db/OracleBulkCopy.cs
	// https://msdn.microsoft.com/en-us/library/system.data.oracleclient.oracletype(v=vs.110).aspx

	private readonly OracleConnection _connection;
	private readonly OracleTransaction? _externalTransaction;

	/// <summary>
	///     Set to true if the BulkCopy object instantiated its own OracleConnection.
	/// </summary>
	private readonly bool _ownsTheConnection;

	private int _batchSize;
	private bool _disposed;

	public OracleBulkCopy(string connectionString)
		: this(new OracleConnection(connectionString), null, true)
	{
	}

	public OracleBulkCopy(OracleConnection connection, OracleTransaction? transaction = null)
		: this(connection, transaction, false)
	{
	}

	private OracleBulkCopy(OracleConnection connection, OracleTransaction? transaction, bool ownsTheConnection)
	{
		ArgumentNullException.ThrowIfNull(connection);

		_connection = connection;
		_externalTransaction = transaction;
		_ownsTheConnection = ownsTheConnection;
	}

	public required string DestinationTableName
	{
		get;
		init
		{
			if (string.IsNullOrWhiteSpace(value))
				throw new InvalidOperationException("DestinationTableName must be set before calling WriteToServer.");

			field = value;
		}
	}

	public int BatchSize
	{
		get => _batchSize;
		set
		{
			if (value < 0) throw new ArgumentOutOfRangeException(nameof(value), "BatchSize must be >= 0.");

			_batchSize = value;
		}
	}

	private bool UploadEverythingInSingleBatch => _batchSize is 0;

	public async ValueTask DisposeAsync()
	{
		if (_disposed) return;

		if (_ownsTheConnection) await _connection.DisposeAsync().ConfigureAwait(false);

		_disposed = true;
		GC.SuppressFinalize(this);
	}

	public void Dispose()
	{
		if (_disposed) return;

		if (_ownsTheConnection) _connection.Dispose();

		_disposed = true;
		GC.SuppressFinalize(this);
	}

	public void WriteToServer(DataTable table)
	{
		ThrowIfDisposed();
		ArgumentNullException.ThrowIfNull(table);

		if (table.Columns.Count is 0)
			throw new ArgumentException("DataTable must contain at least one column.", nameof(table));

		if (table.Rows.Count is 0) return;

		ValidateConnection();
		OpenConnection();

		if (UploadEverythingInSingleBatch)
			WriteToServerInSingleBatch(table);
		else
			WriteToServerInMultipleBatches(table);
	}

	private void WriteToServerInSingleBatch(DataTable table)
	{
		var commandText = BuildCommandText(table);
		WriteSingleBatchOfData(table, 0, commandText, table.Rows.Count);
	}

	private void WriteToServerInMultipleBatches(DataTable table)
	{
		var commandText = BuildCommandText(table);

		for (var skipOffset = 0; skipOffset < table.Rows.Count; skipOffset += BatchSize)
		{
			var currentBatchSize = Math.Min(BatchSize, table.Rows.Count - skipOffset);
			WriteSingleBatchOfData(table, skipOffset, commandText, currentBatchSize);
		}
	}

	private string BuildCommandText(DataTable table)
	{
		var safeTableName = QuoteQualifiedName(DestinationTableName);
		var columnList = GetColumnList(table);
		var valueList = GetValueList(table.Columns.Count);

		return $"INSERT INTO {safeTableName} ({columnList}) VALUES ({valueList})";
	}

	private void WriteSingleBatchOfData(DataTable table, int skipOffset, string commandText, int batchSize)
	{
		var parameters = GetParameters(table, batchSize, skipOffset);

		using var cmd = _connection.CreateCommand();
		// cmd.BindByName = true;
		cmd.CommandText = commandText;
		cmd.ArrayBindCount = batchSize;

		if (_externalTransaction is not null) cmd.Transaction = _externalTransaction;

		foreach (var parameter in parameters) cmd.Parameters.Add(parameter);

		cmd.ExecuteNonQuery();
	}

	private static List<OracleParameter> GetParameters(DataTable data, int batchSize, int skipOffset)
	{
		var parameters = new List<OracleParameter>(data.Columns.Count);

		for (var colIndex = 0; colIndex < data.Columns.Count; colIndex++)
		{
			var column = data.Columns[colIndex];
			var dbType = GetOracleDbTypeFromDotnetType(column.DataType);
			var values = new object[batchSize];

			for (var rowIndex = 0; rowIndex < batchSize; rowIndex++)
			{
				var sourceValue = data.Rows[skipOffset + rowIndex][colIndex];
				values[rowIndex] = sourceValue is null || sourceValue == DBNull.Value ? DBNull.Value : sourceValue;
			}

			parameters.Add(new OracleParameter($":{colIndex + 1}", dbType)
			{
				Value = values
			});
		}

		return parameters;
	}

	private static string GetColumnList(DataTable data)
	{
		var sb = new StringBuilder(data.Columns.Count * 16);

		for (var i = 0; i < data.Columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");

			sb.Append(QuoteIdentifier(data.Columns[i].ColumnName));
		}

		return sb.ToString();
	}

	private static string GetValueList(int columnCount)
	{
		var sb = new StringBuilder(columnCount * 5);

		for (var i = 1; i <= columnCount; i++)
		{
			if (i > 1) sb.Append(", ");

			sb.Append(':');
			sb.Append(i);
		}

		return sb.ToString();
	}

	private static string QuoteQualifiedName(string qualifiedName)
	{
		if (string.IsNullOrWhiteSpace(qualifiedName))
			throw new ArgumentException("Object name cannot be null or empty.", nameof(qualifiedName));

		var parts = qualifiedName.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
		if (parts.Length is 0) throw new ArgumentException("Object name is invalid.", nameof(qualifiedName));

		var sb = new StringBuilder(qualifiedName.Length + 4);
		for (var i = 0; i < parts.Length; i++)
		{
			if (i > 0) sb.Append('.');

			sb.Append(QuoteIdentifier(parts[i].Trim()));
		}

		return sb.ToString();
	}

	private static string QuoteIdentifier(string identifier)
	{
		if (string.IsNullOrWhiteSpace(identifier))
			throw new ArgumentException("Identifier cannot be null or empty.", nameof(identifier));

		var trimmed = identifier.Trim();
		foreach (var ch in trimmed)
		{
			if (char.IsAsciiLetterOrDigit(ch) || ch is '_' or '$' or '#') continue;
			throw new ArgumentException($"Identifier contains unsupported character '{ch}'.", nameof(identifier));
		}

		return $"\"{trimmed}\"";
	}

	private static OracleDbType GetOracleDbTypeFromDotnetType(Type type)
	{
		var t = Nullable.GetUnderlyingType(type) ?? type;

		if (t == typeof(byte[])) return OracleDbType.Blob;
		if (t == typeof(string)) return OracleDbType.Varchar2;
		if (t == typeof(DateTime)) return OracleDbType.Date;
		if (t == typeof(decimal)) return OracleDbType.Decimal;
		if (t == typeof(int)) return OracleDbType.Int32;
		if (t == typeof(long)) return OracleDbType.Int64;
		if (t == typeof(short)) return OracleDbType.Int16;
		if (t == typeof(sbyte) || t == typeof(byte)) return OracleDbType.Byte;
		if (t == typeof(float)) return OracleDbType.Single;
		if (t == typeof(double)) return OracleDbType.Double;
		if (t == typeof(bool)) return OracleDbType.Boolean;
		if (t == typeof(char)) return OracleDbType.Char;

		return OracleDbType.Varchar2;
	}

	private void ValidateConnection()
	{
		if (_externalTransaction is not null && !ReferenceEquals(_externalTransaction.Connection, _connection))
			throw new InvalidOperationException("OracleTransaction does not belong to the provided OracleConnection.");
	}

	private void OpenConnection()
	{
		if (_connection.State is ConnectionState.Open) return;

		if (_ownsTheConnection)
		{
			_connection.Open();
			return;
		}

		throw new InvalidOperationException("The provided OracleConnection must be open before calling WriteToServer.");
	}

	public void Close()
	{
		Dispose();
	}

	private void ThrowIfDisposed()
	{
		ObjectDisposedException.ThrowIf(_disposed, typeof(OracleBulkCopy));
	}
}