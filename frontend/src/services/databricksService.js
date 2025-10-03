import axios from "axios";

// Configure axios defaults
const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000, // 30 seconds timeout
  headers: {
    "Content-Type": "application/json",
  },
});

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
    return config;
  },
  (error) => {
    console.error("API Request Error:", error);
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    console.log(`API Response: ${response.status} ${response.config.url}`);
    return response;
  },
  (error) => {
    console.error("API Response Error:", error.response?.data || error.message);
    return Promise.reject(error);
  }
);

export const databricksService = {
  // Health check
  async healthCheck() {
    try {
      const response = await api.get("/health");
      return response.data;
    } catch (error) {
      throw new Error(`Health check failed: ${error.message}`);
    }
  },

  // Databricks connection
  async connect(config) {
    try {
      const response = await api.post("/api/databricks/connect", config);
      return response.data;
    } catch (error) {
      throw new Error(
        `Connection failed: ${error.response?.data?.detail || error.message}`
      );
    }
  },

  async getStatus() {
    try {
      const response = await api.get("/api/databricks/status");
      return response.data;
    } catch (error) {
      // If status check fails, assume not connected
      return { connected: false, error: error.message };
    }
  },

  // AI configuration
  async configureAI(config) {
    try {
      const response = await api.post("/api/ai/configure", config);
      return response.data;
    } catch (error) {
      throw new Error(
        `AI configuration failed: ${
          error.response?.data?.detail || error.message
        }`
      );
    }
  },

  // PDF management
  async uploadPDF(file, createNotebook = false) {
    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("create_notebook", createNotebook);

      const response = await api.post("/api/pdf/upload", formData, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
        onUploadProgress: (progressEvent) => {
          const percentCompleted = Math.round(
            (progressEvent.loaded * 100) / progressEvent.total
          );
          console.log(`Upload progress: ${percentCompleted}%`);
        },
      });

      return response.data;
    } catch (error) {
      throw new Error(
        `PDF upload failed: ${error.response?.data?.detail || error.message}`
      );
    }
  },

  async listPDFs() {
    try {
      const response = await api.get("/api/pdf/list");
      return response.data;
    } catch (error) {
      throw new Error(
        `Failed to list PDFs: ${error.response?.data?.detail || error.message}`
      );
    }
  },

  // Chat functionality
  async queryPDF(requestData) {
    try {
      // Use the new direct analyze endpoint that mimics single-page-app
      const response = await api.post("/api/analyze/pdf", {
        question: requestData.question,
        pdf_path: requestData.pdf_path,
        conversation_id: requestData.conversation_id,
      });
      return response.data;
    } catch (error) {
      console.error("PDF query error:", error);
      throw new Error(
        `PDF query failed: ${error.response?.data?.detail || error.message}`
      );
    }
  },

  async getConversationHistory(conversationId) {
    try {
      const response = await api.get(`/api/chat/history/${conversationId}`);
      return response.data;
    } catch (error) {
      throw new Error(
        `Failed to get conversation history: ${
          error.response?.data?.detail || error.message
        }`
      );
    }
  },

  async clearConversationHistory(conversationId) {
    try {
      const response = await api.delete(`/api/chat/history/${conversationId}`);
      return response.data;
    } catch (error) {
      throw new Error(
        `Failed to clear conversation history: ${
          error.response?.data?.detail || error.message
        }`
      );
    }
  },
};

// Utility functions
export const formatFileSize = (bytes) => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
};

export const formatDate = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleDateString() + " " + date.toLocaleTimeString();
};

export const generateConversationId = () => {
  return "conv_" + Math.random().toString(36).substr(2, 9) + "_" + Date.now();
};

export default databricksService;
