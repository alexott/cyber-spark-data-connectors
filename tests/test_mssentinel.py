"""Unit tests for Microsoft Sentinel / Azure Monitor data source."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType, TimestampType

from cyber_connectors.MsSentinel import (
    AzureMonitorBatchWriter,
    AzureMonitorDataSource,
    AzureMonitorStreamWriter,
    MicrosoftSentinelDataSource,
)


@pytest.fixture
def basic_options():
    """Basic required options for Azure Monitor data source."""
    return {
        "dce": "https://test-dce.monitor.azure.com",
        "dcr_id": "dcr-test123456789",
        "dcs": "Custom-TestTable_CL",
        "tenant_id": "tenant-id-12345",
        "client_id": "client-id-12345",
        "client_secret": "client-secret-12345",
    }


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )


class TestAzureMonitorDataSource:
    """Test AzureMonitorDataSource class."""

    def test_name(self):
        """Test that data source name is 'azure-monitor'."""
        assert AzureMonitorDataSource.name() == "azure-monitor"

    def test_streamWriter(self, basic_options, sample_schema):
        """Test that streamWriter returns AzureMonitorStreamWriter."""
        ds = AzureMonitorDataSource(options=basic_options)
        writer = ds.streamWriter(sample_schema, overwrite=True)
        assert isinstance(writer, AzureMonitorStreamWriter)

    def test_writer(self, basic_options, sample_schema):
        """Test that writer returns AzureMonitorBatchWriter."""
        ds = AzureMonitorDataSource(options=basic_options)
        writer = ds.writer(sample_schema, overwrite=True)
        assert isinstance(writer, AzureMonitorBatchWriter)


class TestMicrosoftSentinelDataSource:
    """Test MicrosoftSentinelDataSource class."""

    def test_name(self):
        """Test that data source name is 'ms-sentinel'."""
        assert MicrosoftSentinelDataSource.name() == "ms-sentinel"

    def test_streamWriter(self, basic_options, sample_schema):
        """Test that streamWriter returns AzureMonitorStreamWriter."""
        ds = MicrosoftSentinelDataSource(options=basic_options)
        writer = ds.streamWriter(sample_schema, overwrite=True)
        assert isinstance(writer, AzureMonitorStreamWriter)

    def test_writer(self, basic_options, sample_schema):
        """Test that writer returns AzureMonitorBatchWriter."""
        ds = MicrosoftSentinelDataSource(options=basic_options)
        writer = ds.writer(sample_schema, overwrite=True)
        assert isinstance(writer, AzureMonitorBatchWriter)


class TestAzureMonitorWriter:
    """Test AzureMonitorWriter functionality."""

    def test_init_required_options(self, basic_options):
        """Test initialization with required options."""
        writer = AzureMonitorBatchWriter(basic_options)
        assert writer.dce == "https://test-dce.monitor.azure.com"
        assert writer.dcr_id == "dcr-test123456789"
        assert writer.dcs == "Custom-TestTable_CL"
        assert writer.tenant_id == "tenant-id-12345"
        assert writer.client_id == "client-id-12345"
        assert writer.client_secret == "client-secret-12345"
        assert writer.batch_size == 50

    def test_init_missing_dce(self):
        """Test that missing DCE raises assertion error."""
        options = {
            "dcr_id": "dcr-test",
            "dcs": "stream",
            "tenant_id": "tenant",
            "client_id": "client",
            "client_secret": "secret",
        }
        with pytest.raises(AssertionError):
            AzureMonitorBatchWriter(options)

    def test_init_missing_dcr_id(self):
        """Test that missing DCR ID raises assertion error."""
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcs": "stream",
            "tenant_id": "tenant",
            "client_id": "client",
            "client_secret": "secret",
        }
        with pytest.raises(AssertionError):
            AzureMonitorBatchWriter(options)

    def test_init_missing_dcs(self):
        """Test that missing DCS raises assertion error."""
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcr_id": "dcr-test",
            "tenant_id": "tenant",
            "client_id": "client",
            "client_secret": "secret",
        }
        with pytest.raises(AssertionError):
            AzureMonitorBatchWriter(options)

    def test_init_missing_authentication(self):
        """Test that missing authentication raises assertion error."""
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcr_id": "dcr-test",
            "dcs": "stream",
        }
        with pytest.raises(AssertionError, match="Authentication required"):
            AzureMonitorBatchWriter(options)

    def test_init_partial_sp_credentials_fails(self):
        """Test that partial SP credentials fail."""
        # Only tenant_id and client_id provided
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcr_id": "dcr-test",
            "dcs": "stream",
            "tenant_id": "tenant",
            "client_id": "client",
        }
        with pytest.raises(AssertionError, match="Authentication required"):
            AzureMonitorBatchWriter(options)

    def test_init_with_databricks_credential(self):
        """Test initialization with databricks_credential."""
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcr_id": "dcr-test",
            "dcs": "stream",
            "databricks_credential": "my-azure-credential",
        }
        writer = AzureMonitorBatchWriter(options)
        assert writer.databricks_credential == "my-azure-credential"
        assert writer.azure_default_credential is False

    def test_init_with_empty_databricks_credential_fails(self):
        """Test that empty databricks_credential is rejected."""
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcr_id": "dcr-test",
            "dcs": "stream",
            "databricks_credential": "",
        }
        with pytest.raises(AssertionError, match="Authentication required"):
            AzureMonitorBatchWriter(options)

    def test_init_with_azure_default_credential(self):
        """Test initialization with azure_default_credential."""
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcr_id": "dcr-test",
            "dcs": "stream",
            "azure_default_credential": "true",
        }
        writer = AzureMonitorBatchWriter(options)
        assert writer.azure_default_credential is True
        assert writer.databricks_credential is None

    def test_init_with_custom_batch_size(self):
        """Test initialization with custom batch size."""
        options = {
            "dce": "https://test.monitor.azure.com",
            "dcr_id": "dcr-test",
            "dcs": "stream",
            "tenant_id": "tenant",
            "client_id": "client",
            "client_secret": "secret",
            "batch_size": "100",
        }
        writer = AzureMonitorBatchWriter(options)
        assert writer.batch_size == 100

    @patch("pyspark.TaskContext")
    def test_write_basic(self, mock_task_context, basic_options):
        """Test basic write functionality."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        with patch("azure.identity.ClientSecretCredential") as mock_credential, patch(
            "azure.monitor.ingestion.LogsIngestionClient"
        ) as mock_logs_client_class:
            mock_credential_instance = Mock()
            mock_credential.return_value = mock_credential_instance

            mock_logs_client = Mock()
            mock_logs_client.upload = Mock()
            mock_logs_client_class.return_value = mock_logs_client

            writer = AzureMonitorBatchWriter(basic_options)
            rows = [Row(id=1, name="test")]
            commit_msg = writer.write(iter(rows))

            assert commit_msg.partition_id == 0
            assert commit_msg.count == 1
            assert mock_logs_client.upload.called

            call_args = mock_logs_client.upload.call_args
            assert call_args[1]["rule_id"] == "dcr-test123456789"
            assert call_args[1]["stream_name"] == "Custom-TestTable_CL"
            assert len(call_args[1]["logs"]) == 1

    @patch("pyspark.TaskContext")
    def test_write_with_datetime(self, mock_task_context, basic_options):
        """Test write with datetime fields (should be converted to strings)."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        with patch("azure.identity.ClientSecretCredential") as mock_credential, patch(
            "azure.monitor.ingestion.LogsIngestionClient"
        ) as mock_logs_client_class:
            mock_credential_instance = Mock()
            mock_credential.return_value = mock_credential_instance

            mock_logs_client = Mock()
            mock_logs_client.upload = Mock()
            mock_logs_client_class.return_value = mock_logs_client

            writer = AzureMonitorBatchWriter(basic_options)
            timestamp = datetime(2024, 1, 1, 12, 0, 0)
            rows = [Row(id=1, timestamp=timestamp)]
            commit_msg = writer.write(iter(rows))

            assert commit_msg.count == 1
            call_args = mock_logs_client.upload.call_args
            logs = call_args[1]["logs"]
            assert isinstance(logs[0]["timestamp"], str)

    @patch("pyspark.TaskContext")
    def test_write_batching(self, mock_task_context):
        """Test that batching works correctly."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        with patch("azure.identity.ClientSecretCredential") as mock_credential, patch(
            "azure.monitor.ingestion.LogsIngestionClient"
        ) as mock_logs_client_class:
            mock_credential_instance = Mock()
            mock_credential.return_value = mock_credential_instance

            mock_logs_client = Mock()
            mock_logs_client.upload = Mock()
            mock_logs_client_class.return_value = mock_logs_client

            options = {
                "dce": "https://test.monitor.azure.com",
                "dcr_id": "dcr-test",
                "dcs": "stream",
                "tenant_id": "tenant",
                "client_id": "client",
                "client_secret": "secret",
                "batch_size": "2",
            }
            writer = AzureMonitorBatchWriter(options)
            rows = [Row(id=i) for i in range(5)]
            commit_msg = writer.write(iter(rows))

            assert commit_msg.count == 5
            # Should be called 3 times: 2+2+1
            assert mock_logs_client.upload.call_count == 3

    @patch("pyspark.TaskContext")
    def test_write_credential_creation(self, mock_task_context, basic_options):
        """Test that credentials are created correctly."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        with patch("azure.identity.ClientSecretCredential") as mock_credential, patch(
            "azure.monitor.ingestion.LogsIngestionClient"
        ) as mock_logs_client_class:
            mock_credential_instance = Mock()
            mock_credential.return_value = mock_credential_instance

            mock_logs_client = Mock()
            mock_logs_client.upload = Mock()
            mock_logs_client_class.return_value = mock_logs_client

            writer = AzureMonitorBatchWriter(basic_options)
            rows = [Row(id=1)]
            writer.write(iter(rows))

            # Check that ClientSecretCredential was called with correct parameters
            mock_credential.assert_called_once_with(
                tenant_id="tenant-id-12345", client_id="client-id-12345", client_secret="client-secret-12345"
            )

            # Check that LogsIngestionClient was created with correct parameters
            mock_logs_client_class.assert_called_once_with(
                "https://test-dce.monitor.azure.com", mock_credential_instance
            )


class TestAzureMonitorStreamWriter:
    """Test AzureMonitorStreamWriter functionality."""

    def test_commit(self, basic_options):
        """Test commit method."""
        writer = AzureMonitorStreamWriter(basic_options)
        messages = [Mock(count=10), Mock(count=20)]
        writer.commit(messages, batchId=1)

    def test_abort(self, basic_options):
        """Test abort method."""
        writer = AzureMonitorStreamWriter(basic_options)
        messages = [Mock(count=10)]
        writer.abort(messages, batchId=1)
