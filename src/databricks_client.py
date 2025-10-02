"""
Databricks client module for handling connections and file operations.
"""
import os
import base64
import logging
from typing import Optional, Dict, Any, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service import workspace
import requests

logger = logging.getLogger(__name__)


class DatabricksClient:
    """Client for interacting with Databricks workspace and APIs."""
    
    def __init__(self, host: str = None, token: str = None):
        """
        Initialize Databricks client.
        
        Args:
            host: Databricks workspace URL
            token: Personal access token
        """
        self.host = host or os.getenv('DATABRICKS_HOST')
        self.token = token or os.getenv('DATABRICKS_TOKEN')
        
        if not self.host or not self.token:
            raise ValueError("Databricks host and token must be provided")
        
        # Initialize the workspace client
        self.config = Config(host=self.host, token=self.token)
        self.workspace_client = WorkspaceClient(config=self.config)
        
        # Set up headers for direct API calls
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the connection to Databricks workspace.
        
        Returns:
            Dict with connection status and user info
        """
        try:
            current_user = self.workspace_client.current_user.me()
            return {
                'success': True,
                'user': current_user.user_name,
                'workspace_url': self.host
            }
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def upload_file_to_workspace(self, file_content: bytes, workspace_path: str, 
                                overwrite: bool = True) -> Dict[str, Any]:
        """
        Upload a file to Databricks workspace.
        
        Args:
            file_content: File content as bytes
            workspace_path: Target path in workspace
            overwrite: Whether to overwrite existing files
            
        Returns:
            Dict with upload status and details
        """
        try:
            # Encode file content to base64
            encoded_content = base64.b64encode(file_content).decode('utf-8')
            
            # Use workspace API to upload
            self.workspace_client.workspace.upload(
                path=workspace_path,
                content=encoded_content,
                format=workspace.ImportFormat.AUTO,
                overwrite=overwrite
            )
            
            return {
                'success': True,
                'path': workspace_path,
                'message': f'File uploaded successfully to {workspace_path}'
            }
            
        except Exception as e:
            logger.error(f"File upload failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def create_notebook_from_template(self, notebook_path: str, template_content: str,
                                    overwrite: bool = True) -> Dict[str, Any]:
        """
        Create a notebook in Databricks workspace.
        
        Args:
            notebook_path: Path for the new notebook
            template_content: Notebook content
            overwrite: Whether to overwrite existing notebook
            
        Returns:
            Dict with creation status
        """
        try:
            self.workspace_client.workspace.upload(
                path=notebook_path,
                content=base64.b64encode(template_content.encode()).decode(),
                format=workspace.ImportFormat.AUTO,
                overwrite=overwrite
            )
            
            return {
                'success': True,
                'path': notebook_path,
                'message': f'Notebook created successfully at {notebook_path}'
            }
            
        except Exception as e:
            logger.error(f"Notebook creation failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def list_workspace_files(self, path: str = "/") -> List[Dict[str, Any]]:
        """
        List files in a workspace directory.
        
        Args:
            path: Workspace path to list
            
        Returns:
            List of file information dictionaries
        """
        try:
            objects = self.workspace_client.workspace.list(path)
            files = []
            
            for obj in objects:
                files.append({
                    'path': obj.path,
                    'object_type': obj.object_type.value if obj.object_type else 'unknown',
                    'language': obj.language.value if obj.language else None
                })
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list workspace files: {str(e)}")
            return []
    
    def export_workspace_file(self, workspace_path: str) -> Optional[bytes]:
        """
        Export/download a file from Databricks workspace.

        Args:
            workspace_path: Path to file in workspace

        Returns:
            File content as bytes or None if failed
        """
        try:
            # Use the workspace export API to get file content
            # Try different export formats for PDF files
            exported_content = None

            # For PDF files, try SOURCE format first (raw binary)
            if workspace_path.lower().endswith('.pdf'):
                try:
                    exported_content = self.workspace_client.workspace.export(
                        path=workspace_path,
                        format=workspace.ExportFormat.SOURCE
                    )
                except Exception as e:
                    logger.warning(f"SOURCE format failed for {workspace_path}: {e}")

            # Fallback to other formats if SOURCE fails
            if not exported_content:
                for format_type in [workspace.ExportFormat.SOURCE, workspace.ExportFormat.HTML, workspace.ExportFormat.JUPYTER]:
                    try:
                        exported_content = self.workspace_client.workspace.export(
                            path=workspace_path,
                            format=format_type
                        )
                        if exported_content and exported_content.content:
                            logger.info(f"Successfully exported {workspace_path} using {format_type}")
                            break
                    except Exception as e:
                        logger.debug(f"Export format {format_type} failed for {workspace_path}: {e}")
                        continue

            if exported_content and exported_content.content:
                # The content might be base64 encoded string or already bytes
                try:
                    # First check if it's already bytes and looks like base64
                    if isinstance(exported_content.content, bytes):
                        logger.info(f"Content is bytes, first 20: {exported_content.content[:20]}")
                        # Check if it looks like base64 encoded content (PDF starts with JVBERi in base64)
                        starts_with_jvberi = exported_content.content.startswith(b'JVBERi')
                        logger.info(f"Starts with JVBERi: {starts_with_jvberi}")

                        if starts_with_jvberi:  # '%PDF' in base64
                            logger.info(f"Detected base64 encoded PDF, decoding...")
                            # Decode the base64
                            file_content = base64.b64decode(exported_content.content)
                            logger.info(f"Successfully decoded base64 content from {workspace_path} ({len(file_content)} bytes)")
                            return file_content
                        else:
                            # Already proper binary content
                            logger.info(f"Successfully exported binary file from {workspace_path} ({len(exported_content.content)} bytes)")
                            return exported_content.content

                    # If it's a string, try to decode as base64
                    file_content = base64.b64decode(exported_content.content)
                    logger.info(f"Successfully decoded base64 string from {workspace_path} ({len(file_content)} bytes)")
                    return file_content

                except Exception as decode_error:
                    # If base64 decode fails, the content might be plain text
                    logger.warning(f"Base64 decode failed, trying direct content: {decode_error}")
                    if isinstance(exported_content.content, str):
                        # For text files, encode as UTF-8
                        return exported_content.content.encode('utf-8')
                    else:
                        # Last resort - return as is
                        return exported_content.content
            else:
                logger.warning(f"No content returned from workspace export: {workspace_path}")
                # Try alternative download method for PDFs
                return self._download_file_direct(workspace_path)

        except Exception as e:
            logger.error(f"Failed to export file from {workspace_path}: {str(e)}")
            # Try alternative download method as fallback
            return self._download_file_direct(workspace_path)

    def _download_file_direct(self, workspace_path: str) -> Optional[bytes]:
        """
        Alternative method to download files using direct API calls.

        Args:
            workspace_path: Path to file in workspace

        Returns:
            File content as bytes or None if failed
        """
        try:
            logger.info(f"Attempting direct download of {workspace_path}")

            # Try using the workspace client's download method if available
            try:
                response = self.workspace_client.workspace.download(workspace_path)
                if response:
                    logger.info(f"Successfully downloaded file from {workspace_path} ({len(response)} bytes)")
                    return response
            except AttributeError:
                logger.debug("Download method not available, trying REST API")
            except Exception as e:
                logger.debug(f"Download method failed: {e}")

            # Fallback to REST API call
            return self._download_via_rest_api(workspace_path)

        except Exception as e:
            logger.error(f"Direct download failed for {workspace_path}: {str(e)}")
            return None

    def _download_via_rest_api(self, workspace_path: str) -> Optional[bytes]:
        """
        Download file using direct REST API calls.

        Args:
            workspace_path: Path to file in workspace

        Returns:
            File content as bytes or None if failed
        """
        try:
            logger.info(f"Attempting REST API download of {workspace_path}")

            # Get the workspace URL and token from the client config
            host = self.workspace_client.config.host
            token = self.workspace_client.config.token

            # Construct the export API URL
            url = f"{host}/api/2.0/workspace/export"

            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            # Try different formats for PDF files
            for format_type in ['SOURCE', 'AUTO']:
                try:
                    payload = {
                        'path': workspace_path,
                        'format': format_type
                    }

                    response = requests.get(url, headers=headers, params=payload)

                    if response.status_code == 200:
                        result = response.json()
                        if 'content' in result:
                            # Decode base64 content
                            file_content = base64.b64decode(result['content'])
                            logger.info(f"Successfully downloaded via REST API: {workspace_path} ({len(file_content)} bytes)")
                            return file_content
                    else:
                        logger.debug(f"REST API format {format_type} failed with status {response.status_code}")

                except Exception as e:
                    logger.debug(f"REST API format {format_type} failed: {e}")
                    continue

            logger.warning(f"All REST API download methods failed for {workspace_path}")
            return None

        except Exception as e:
            logger.error(f"REST API download failed for {workspace_path}: {str(e)}")
            return None

    def execute_sql_query(self, sql_query: str, warehouse_id: str = None) -> Dict[str, Any]:
        """
        Execute a SQL query using Databricks SQL warehouse.

        Args:
            sql_query: SQL query to execute
            warehouse_id: Optional warehouse ID (uses default if not provided)

        Returns:
            Dict with execution results
        """
        try:
            # Import SQL execution client
            from databricks.sdk.service import sql

            # Get available warehouses if no warehouse_id provided
            if not warehouse_id:
                warehouses = list(self.workspace_client.warehouses.list())
                if not warehouses:
                    return {
                        'success': False,
                        'error': 'No SQL warehouses available'
                    }
                warehouse_id = warehouses[0].id
                logger.info(f"Using warehouse: {warehouse_id}")

            # Execute the query
            logger.info(f"Executing SQL query on warehouse {warehouse_id}")

            # Create a statement execution
            statement = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql_query,
                wait_timeout="30s"
            )

            # Wait for completion and get results
            if statement.status.state == sql.StatementState.SUCCEEDED:
                # Extract results
                result_data = []
                if statement.result and statement.result.data_array:
                    # Debug: Log result structure
                    logger.info(f"Result type: {type(statement.result)}")
                    logger.info(f"Result attributes: {dir(statement.result)}")

                    # Get column names - handle different result formats
                    columns = []
                    try:
                        if hasattr(statement.result, 'schema') and statement.result.schema:
                            columns = [col.name for col in statement.result.schema.columns]
                            logger.info(f"Found schema with {len(columns)} columns")
                        elif hasattr(statement.result, 'manifest') and statement.result.manifest:
                            # Alternative schema location
                            if hasattr(statement.result.manifest, 'schema'):
                                columns = [col.name for col in statement.result.manifest.schema.columns]
                                logger.info(f"Found manifest schema with {len(columns)} columns")
                    except Exception as schema_error:
                        logger.warning(f"Schema extraction failed: {schema_error}")
                        # Fallback: use generic column names
                        if statement.result.data_array:
                            first_row = statement.result.data_array[0]
                            columns = [f"col_{i}" for i in range(len(first_row))]
                            logger.info(f"Using generic column names: {columns}")

                    # Process rows
                    for row in statement.result.data_array:
                        row_dict = {}
                        for i, value in enumerate(row):
                            column_name = columns[i] if i < len(columns) else f"col_{i}"
                            row_dict[column_name] = value
                        result_data.append(row_dict)

                logger.info(f"SQL query executed successfully, {len(result_data)} rows returned")
                return {
                    'success': True,
                    'data': result_data,
                    'statement_id': statement.statement_id,
                    'warehouse_id': warehouse_id
                }
            else:
                error_msg = f"Query failed with state: {statement.status.state}"
                if statement.status.error:
                    error_msg += f", Error: {statement.status.error.message}"

                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg,
                    'statement_id': statement.statement_id
                }

        except Exception as e:
            logger.error(f"Failed to execute SQL query: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def get_clusters(self) -> List[Dict[str, Any]]:
        """
        Get list of available clusters.

        Returns:
            List of cluster information
        """
        try:
            clusters = self.workspace_client.clusters.list()
            cluster_list = []

            for cluster in clusters:
                cluster_list.append({
                    'cluster_id': cluster.cluster_id,
                    'cluster_name': cluster.cluster_name,
                    'state': cluster.state.value if cluster.state else 'unknown',
                    'node_type_id': cluster.node_type_id
                })

            return cluster_list

        except Exception as e:
            logger.error(f"Failed to get clusters: {str(e)}")
            return []
