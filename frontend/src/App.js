import React, { useState, useEffect } from "react";
import {
  BrowserRouter as Router,
  Routes,
  Route,
  Navigate,
} from "react-router-dom";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import { CssBaseline, Box } from "@mui/material";
import { Toaster } from "react-hot-toast";

// Components
import Navbar from "./components/Navbar";
import Sidebar from "./components/Sidebar";
import Dashboard from "./pages/Dashboard";
import PDFUpload from "./pages/PDFUpload";
import ChatInterface from "./pages/ChatInterface";
import AnalyzePage from "./pages/AnalyzePage";
import Settings from "./pages/Settings";

// Services
import { databricksService } from "./services/databricksService";

// Theme configuration
const theme = createTheme({
  palette: {
    mode: "light",
    primary: {
      main: "#1976d2",
      light: "#42a5f5",
      dark: "#1565c0",
    },
    secondary: {
      main: "#dc004e",
    },
    background: {
      default: "#f5f5f5",
      paper: "#ffffff",
    },
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    h4: {
      fontWeight: 600,
    },
    h6: {
      fontWeight: 500,
    },
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: "none",
          borderRadius: 8,
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: 12,
          boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
        },
      },
    },
  },
});

function App() {
  const [databricksConnected, setDatabricksConnected] = useState(false);
  const [aiConfigured, setAiConfigured] = useState(false);
  const [loading, setLoading] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Check connection status and auto-connect on app load
  useEffect(() => {
    initializeApp();
  }, []);

  const initializeApp = async () => {
    try {
      // First check if already connected
      const statusResponse = await databricksService.getStatus();
      if (statusResponse.connected) {
        setDatabricksConnected(true);
        setLoading(false);
        return;
      }

      // Try auto-connect if environment variables are set
      const autoHost = process.env.REACT_APP_DATABRICKS_HOST;
      const autoToken = process.env.REACT_APP_DATABRICKS_TOKEN;

      if (autoHost && autoToken) {
        console.log("Attempting auto-connection to Databricks...");
        const connectResult = await handleDatabricksConnect({
          host: autoHost,
          token: autoToken,
        });

        if (connectResult.success) {
          console.log("Auto-connection successful");

          // Auto-configure AI if settings are provided
          const aiProvider = process.env.REACT_APP_AI_PROVIDER || "databricks";
          const aiModel =
            process.env.REACT_APP_AI_MODEL || "databricks-gpt-oss-120b";

          const aiResult = await handleAiConfigure({
            provider: aiProvider,
            model: aiModel,
          });

          if (aiResult.success) {
            console.log("Auto-AI configuration successful");
          }
        }
      }

      setLoading(false);
    } catch (error) {
      console.error("Failed to initialize app:", error);
      setDatabricksConnected(false);
      setLoading(false);
    }
  };

  const handleDatabricksConnect = async (config) => {
    try {
      const result = await databricksService.connect(config);
      if (result.success) {
        setDatabricksConnected(true);
        return { success: true, message: result.message };
      } else {
        return { success: false, error: result.error };
      }
    } catch (error) {
      return { success: false, error: error.message };
    }
  };

  const handleAiConfigure = async (config) => {
    try {
      const result = await databricksService.configureAI(config);
      if (result.success) {
        setAiConfigured(true);
        return { success: true, message: result.message };
      } else {
        return { success: false, error: result.error };
      }
    } catch (error) {
      return { success: false, error: error.message };
    }
  };

  const toggleSidebar = () => {
    setSidebarOpen(!sidebarOpen);
  };

  if (loading) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box
          display="flex"
          justifyContent="center"
          alignItems="center"
          minHeight="100vh"
        >
          <div>Loading...</div>
        </Box>
      </ThemeProvider>
    );
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
        <Box sx={{ display: "flex", minHeight: "100vh" }}>
          {/* Sidebar */}
          <Sidebar
            open={sidebarOpen}
            onClose={() => setSidebarOpen(false)}
            databricksConnected={databricksConnected}
            aiConfigured={aiConfigured}
          />

          {/* Main content */}
          <Box sx={{ flexGrow: 1, display: "flex", flexDirection: "column" }}>
            {/* Navbar */}
            <Navbar
              onMenuClick={toggleSidebar}
              databricksConnected={databricksConnected}
              aiConfigured={aiConfigured}
            />

            {/* Page content */}
            <Box sx={{ flexGrow: 1, p: 3 }}>
              <Routes>
                <Route
                  path="/"
                  element={
                    <Dashboard
                      databricksConnected={databricksConnected}
                      aiConfigured={aiConfigured}
                    />
                  }
                />
                <Route
                  path="/upload"
                  element={
                    databricksConnected ? (
                      <PDFUpload />
                    ) : (
                      <Navigate to="/settings" replace />
                    )
                  }
                />
                <Route
                  path="/chat"
                  element={
                    databricksConnected && aiConfigured ? (
                      <ChatInterface />
                    ) : (
                      <Navigate to="/settings" replace />
                    )
                  }
                />
                <Route
                  path="/analyze"
                  element={
                    databricksConnected && aiConfigured ? (
                      <AnalyzePage />
                    ) : (
                      <Navigate to="/settings" replace />
                    )
                  }
                />
                <Route
                  path="/settings"
                  element={
                    <Settings
                      databricksConnected={databricksConnected}
                      aiConfigured={aiConfigured}
                      onDatabricksConnect={handleDatabricksConnect}
                      onAiConfigure={handleAiConfigure}
                    />
                  }
                />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </Box>
          </Box>
        </Box>

        {/* Toast notifications */}
        <Toaster
          position="top-right"
          toastOptions={{
            duration: 4000,
            style: {
              background: "#363636",
              color: "#fff",
            },
            success: {
              duration: 3000,
              theme: {
                primary: "#4aed88",
              },
            },
          }}
        />
      </Router>
    </ThemeProvider>
  );
}

export default App;
