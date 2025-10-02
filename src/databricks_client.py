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
            exported_content = self.workspace_client.workspace.export(
                path=workspace_path,
                format=workspace.ExportFormat.AUTO
            )

            if exported_content and exported_content.content:
                # The content is already base64 encoded string, decode it to bytes
                try:
                    file_content = base64.b64decode(exported_content.content)
                    logger.info(f"Successfully exported file from {workspace_path} ({len(file_content)} bytes)")
                    return file_content
                except Exception as decode_error:
                    # If base64 decode fails, the content might already be bytes
                    logger.warning(f"Base64 decode failed, trying direct content: {decode_error}")
                    if isinstance(exported_content.content, bytes):
                        return exported_content.content
                    else:
                        # Try encoding the string as bytes
                        return exported_content.content.encode('utf-8')
            else:
                logger.warning(f"No content returned from workspace export: {workspace_path}")
                return None

        except Exception as e:
            logger.error(f"Failed to export file from {workspace_path}: {str(e)}")
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
