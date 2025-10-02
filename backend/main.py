"""
FastAPI Backend Server for Databricks PDF Processing
Replaces Streamlit with REST API endpoints
"""
import os
import sys
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.databricks_api import DatabricksAPIIntegration
from src.pdf_manager import PDFManager
from src.databricks_ai_engine import DatabricksAIEngine
from config import Config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Databricks PDF Processing API",
    description="REST API for PDF upload, processing, and AI-powered querying using Databricks",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for connections
databricks_api: Optional[DatabricksAPIIntegration] = None
pdf_manager: Optional[PDFManager] = None
ai_engine: Optional[DatabricksAIEngine] = None

# Pydantic models for request/response
class ConnectionConfig(BaseModel):
    host: str
    token: str

class AIConfig(BaseModel):
    provider: str  # "databricks" or "openai"
    model: str
    cluster_id: Optional[str] = None
    openai_api_key: Optional[str] = None

class ChatMessage(BaseModel):
    question: str
    pdf_path: str
    conversation_id: Optional[str] = None

class ChatResponse(BaseModel):
    success: bool
    answer: Optional[str] = None
    conversation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

# Dependency to get databricks connection
async def get_databricks_connection():
    global databricks_api
    if not databricks_api:
        raise HTTPException(status_code=400, detail="Databricks connection not established")
    return databricks_api

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Databricks PDF Processing API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "databricks_connected": databricks_api is not None
    }

@app.post("/api/databricks/connect")
async def connect_databricks(config: ConnectionConfig):
    """Establish connection to Databricks."""
    global databricks_api, pdf_manager, ai_engine
    
    try:
        # Initialize Databricks API integration
        databricks_api = DatabricksAPIIntegration(config.host, config.token)
        
        # Test connection
        connection_result = databricks_api.test_connection()
        
        if connection_result['success']:
            # Initialize PDF manager and AI engine
            pdf_manager = PDFManager(databricks_api.client)
            ai_engine = DatabricksAIEngine(databricks_api.client)
            
            return {
                "success": True,
                "message": "Connected to Databricks successfully",
                "user": connection_result['user'],
                "workspace_url": connection_result['workspace_url']
            }
        else:
            return {
                "success": False,
                "error": connection_result['error']
            }
            
    except Exception as e:
        logger.error(f"Failed to connect to Databricks: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/api/databricks/status")
async def get_databricks_status(db: DatabricksAPIIntegration = Depends(get_databricks_connection)):
    """Get Databricks connection status and workspace info."""
    try:
        # Get workspace info
        clusters = db.get_cluster_info()
        
        return {
            "connected": True,
            "workspace_info": {
                "clusters": clusters,
                "timestamp": datetime.now().isoformat()
            }
        }
    except Exception as e:
        return {
            "connected": False,
            "error": str(e)
        }

@app.post("/api/ai/configure")
async def configure_ai(config: AIConfig, db: DatabricksAPIIntegration = Depends(get_databricks_connection)):
    """Configure AI provider and settings."""
    global ai_engine
    
    try:
        if config.provider == "databricks":
            # Configure Databricks AI
            ai_engine = DatabricksAIEngine(
                databricks_client=db.client,
                model=config.model
            )
            
            return {
                "success": True,
                "provider": "databricks",
                "model": config.model,
                "message": "Databricks AI configured successfully"
            }
        
        elif config.provider == "openai":
            # Configure OpenAI (if needed)
            if not config.openai_api_key:
                return {
                    "success": False,
                    "error": "OpenAI API key required"
                }
            
            # Set OpenAI API key in environment
            os.environ['OPENAI_API_KEY'] = config.openai_api_key
            
            return {
                "success": True,
                "provider": "openai",
                "model": config.model,
                "message": "OpenAI configured successfully"
            }
        
        else:
            return {
                "success": False,
                "error": f"Unsupported AI provider: {config.provider}"
            }
            
    except Exception as e:
        logger.error(f"Failed to configure AI: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@app.post("/api/pdf/upload")
async def upload_pdf(
    file: UploadFile = File(...),
    create_notebook: bool = Form(False),
    db: DatabricksAPIIntegration = Depends(get_databricks_connection)
):
    """Upload PDF to Databricks workspace."""
    try:
        # Validate file type
        if not file.filename.lower().endswith('.pdf'):
            raise HTTPException(status_code=400, detail="Only PDF files are allowed")
        
        # Read file content
        file_content = await file.read()
        
        # Upload using existing workflow
        result = db.upload_pdf_workflow(
            file_content=file_content,
            filename=file.filename,
            create_processing_notebook=create_notebook
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to upload PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pdf/list")
async def list_pdfs(db: DatabricksAPIIntegration = Depends(get_databricks_connection)):
    """List uploaded PDFs in Databricks workspace."""
    global pdf_manager
    
    try:
        if not pdf_manager:
            pdf_manager = PDFManager(db.client)
        
        pdfs = pdf_manager.list_available_pdfs()
        
        return {
            "success": True,
            "pdfs": pdfs,
            "count": len(pdfs)
        }
        
    except Exception as e:
        logger.error(f"Failed to list PDFs: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@app.post("/api/chat/query", response_model=ChatResponse)
async def query_pdf(
    message: ChatMessage,
    db: DatabricksAPIIntegration = Depends(get_databricks_connection)
):
    """Query PDF using AI."""
    global ai_engine, pdf_manager
    
    try:
        if not ai_engine:
            raise HTTPException(status_code=400, detail="AI engine not configured")
        
        if not pdf_manager:
            pdf_manager = PDFManager(db.client)
        
        # Get PDF content
        pdf_content = pdf_manager.get_pdf_content(message.pdf_path)
        if not pdf_content:
            raise HTTPException(status_code=404, detail="PDF content not found")
        
        # Generate conversation ID if not provided
        conversation_id = message.conversation_id or str(uuid.uuid4())
        
        # Query using AI engine
        result = ai_engine.query_pdf_with_databricks_ai(
            file_content=pdf_content,
            question=message.question,
            conversation_id=conversation_id
        )
        
        return ChatResponse(
            success=result['success'],
            answer=result.get('answer'),
            conversation_id=conversation_id,
            metadata=result.get('metadata'),
            error=result.get('error')
        )
        
    except Exception as e:
        logger.error(f"Failed to query PDF: {str(e)}")
        return ChatResponse(
            success=False,
            error=str(e)
        )

@app.get("/api/chat/history/{conversation_id}")
async def get_conversation_history(conversation_id: str):
    """Get conversation history for a specific conversation."""
    global ai_engine
    
    try:
        if not ai_engine:
            raise HTTPException(status_code=400, detail="AI engine not configured")
        
        history = ai_engine.get_conversation_history(conversation_id)
        
        return {
            "success": True,
            "conversation_id": conversation_id,
            "history": history
        }
        
    except Exception as e:
        logger.error(f"Failed to get conversation history: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@app.delete("/api/chat/history/{conversation_id}")
async def clear_conversation_history(conversation_id: str):
    """Clear conversation history for a specific conversation."""
    global ai_engine
    
    try:
        if not ai_engine:
            raise HTTPException(status_code=400, detail="AI engine not configured")
        
        ai_engine.clear_conversation_context(conversation_id)
        
        return {
            "success": True,
            "message": f"Conversation {conversation_id} cleared"
        }
        
    except Exception as e:
        logger.error(f"Failed to clear conversation history: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

if __name__ == "__main__":
    # Run the server
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
