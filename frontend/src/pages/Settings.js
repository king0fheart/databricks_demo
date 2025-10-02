import React, { useState } from "react";
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  Grid,
  Alert,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  Chip,
} from "@mui/material";
import {
  Storage,
  SmartToy,
  Save,
  CheckCircle,
  Error,
} from "@mui/icons-material";
import toast from "react-hot-toast";

const Settings = ({
  databricksConnected,
  aiConfigured,
  onDatabricksConnect,
  onAiConfigure,
}) => {
  // Databricks connection state
  const [databricksConfig, setDatabricksConfig] = useState({
    host: "",
    token: "",
  });
  const [databricksLoading, setDatabricksLoading] = useState(false);
  const [databricksTestResult, setDatabricksTestResult] = useState(null);

  // AI configuration state
  const [aiConfig, setAiConfig] = useState({
    provider: "databricks",
    model: "databricks-gpt-oss-120b",
    cluster_id: "",
    openai_api_key: "",
  });
  const [aiLoading, setAiLoading] = useState(false);
  const [aiTestResult, setAiTestResult] = useState(null);

  const databricksModels = [
    "databricks-gpt-oss-120b",
    "databricks-llama-2-70b-chat",
    "databricks-mpt-30b-instruct",
  ];

  const openaiModels = ["gpt-4", "gpt-4-turbo-preview", "gpt-3.5-turbo"];

  const handleDatabricksConfigChange = (field, value) => {
    setDatabricksConfig((prev) => ({
      ...prev,
      [field]: value,
    }));
    setDatabricksTestResult(null);
  };

  const handleAiConfigChange = (field, value) => {
    setAiConfig((prev) => ({
      ...prev,
      [field]: value,
    }));
    setAiTestResult(null);
  };

  const testDatabricksConnection = async () => {
    if (!databricksConfig.host || !databricksConfig.token) {
      toast.error("Please fill in all Databricks fields");
      return;
    }

    setDatabricksLoading(true);
    setDatabricksTestResult(null);

    try {
      const result = await onDatabricksConnect(databricksConfig);
      setDatabricksTestResult(result);

      if (result.success) {
        toast.success("Databricks connection successful!");
      } else {
        toast.error(result.error || "Connection failed");
      }
    } catch (error) {
      const errorResult = { success: false, error: error.message };
      setDatabricksTestResult(errorResult);
      toast.error("Connection test failed");
    } finally {
      setDatabricksLoading(false);
    }
  };

  const configureAI = async () => {
    if (aiConfig.provider === "openai" && !aiConfig.openai_api_key) {
      toast.error("Please provide OpenAI API key");
      return;
    }

    setAiLoading(true);
    setAiTestResult(null);

    try {
      const result = await onAiConfigure(aiConfig);
      setAiTestResult(result);

      if (result.success) {
        toast.success("AI configuration successful!");
      } else {
        toast.error(result.error || "AI configuration failed");
      }
    } catch (error) {
      const errorResult = { success: false, error: error.message };
      setAiTestResult(errorResult);
      toast.error("AI configuration failed");
    } finally {
      setAiLoading(false);
    }
  };

  return (
    <Box>
      {/* Header */}
      <Box mb={4}>
        <Typography variant="h4" gutterBottom>
          Settings
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Configure your Databricks connection and AI settings
        </Typography>
      </Box>

      <Grid container spacing={3}>
        {/* Databricks Configuration */}
        <Grid item xs={12} lg={6}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" mb={3}>
                <Storage color="primary" sx={{ mr: 1 }} />
                <Typography variant="h6">Databricks Connection</Typography>
                <Box flexGrow={1} />
                <Chip
                  label={databricksConnected ? "Connected" : "Not Connected"}
                  color={databricksConnected ? "success" : "error"}
                  size="small"
                />
              </Box>

              <Box mb={3}>
                <TextField
                  fullWidth
                  label="Databricks Host"
                  placeholder="https://your-workspace.cloud.databricks.com"
                  value={databricksConfig.host}
                  onChange={(e) =>
                    handleDatabricksConfigChange("host", e.target.value)
                  }
                  margin="normal"
                  helperText="Your Databricks workspace URL"
                />

                <TextField
                  fullWidth
                  label="Access Token"
                  type="password"
                  placeholder="dapi..."
                  value={databricksConfig.token}
                  onChange={(e) =>
                    handleDatabricksConfigChange("token", e.target.value)
                  }
                  margin="normal"
                  helperText="Personal access token from Databricks"
                />
              </Box>

              <Box display="flex" gap={2} mb={2}>
                <Button
                  variant="contained"
                  onClick={testDatabricksConnection}
                  disabled={
                    databricksLoading ||
                    !databricksConfig.host ||
                    !databricksConfig.token
                  }
                  startIcon={
                    databricksLoading ? (
                      <CircularProgress size={16} />
                    ) : (
                      <Error />
                    )
                  }
                >
                  {databricksLoading ? "Testing..." : "Test Connection"}
                </Button>
              </Box>

              {databricksTestResult && (
                <Alert
                  severity={databricksTestResult.success ? "success" : "error"}
                  icon={
                    databricksTestResult.success ? <CheckCircle /> : <Error />
                  }
                >
                  {databricksTestResult.success
                    ? `Connected successfully! User: ${databricksTestResult.message}`
                    : `Connection failed: ${databricksTestResult.error}`}
                </Alert>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* AI Configuration */}
        <Grid item xs={12} lg={6}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" mb={3}>
                <SmartToy color="primary" sx={{ mr: 1 }} />
                <Typography variant="h6">AI Configuration</Typography>
                <Box flexGrow={1} />
                <Chip
                  label={aiConfigured ? "Configured" : "Not Configured"}
                  color={aiConfigured ? "success" : "error"}
                  size="small"
                />
              </Box>

              <Box mb={3}>
                <FormControl fullWidth margin="normal">
                  <InputLabel>AI Provider</InputLabel>
                  <Select
                    value={aiConfig.provider}
                    onChange={(e) =>
                      handleAiConfigChange("provider", e.target.value)
                    }
                    label="AI Provider"
                  >
                    <MenuItem value="databricks">Databricks AI</MenuItem>
                    <MenuItem value="openai">OpenAI</MenuItem>
                  </Select>
                </FormControl>

                {aiConfig.provider === "databricks" && (
                  <FormControl fullWidth margin="normal">
                    <InputLabel>Model</InputLabel>
                    <Select
                      value={aiConfig.model}
                      onChange={(e) =>
                        handleAiConfigChange("model", e.target.value)
                      }
                      label="Model"
                    >
                      {databricksModels.map((model) => (
                        <MenuItem key={model} value={model}>
                          {model}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                )}

                {aiConfig.provider === "openai" && (
                  <>
                    <FormControl fullWidth margin="normal">
                      <InputLabel>Model</InputLabel>
                      <Select
                        value={aiConfig.model}
                        onChange={(e) =>
                          handleAiConfigChange("model", e.target.value)
                        }
                        label="Model"
                      >
                        {openaiModels.map((model) => (
                          <MenuItem key={model} value={model}>
                            {model}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>

                    <TextField
                      fullWidth
                      label="OpenAI API Key"
                      type="password"
                      placeholder="sk-..."
                      value={aiConfig.openai_api_key}
                      onChange={(e) =>
                        handleAiConfigChange("openai_api_key", e.target.value)
                      }
                      margin="normal"
                      helperText="Your OpenAI API key"
                    />
                  </>
                )}
              </Box>

              <Box display="flex" gap={2} mb={2}>
                <Button
                  variant="contained"
                  onClick={configureAI}
                  disabled={
                    aiLoading ||
                    !databricksConnected ||
                    (aiConfig.provider === "openai" && !aiConfig.openai_api_key)
                  }
                  startIcon={
                    aiLoading ? <CircularProgress size={16} /> : <Save />
                  }
                >
                  {aiLoading ? "Configuring..." : "Configure AI"}
                </Button>
              </Box>

              {!databricksConnected && (
                <Alert severity="warning" sx={{ mb: 2 }}>
                  Please connect to Databricks first before configuring AI
                </Alert>
              )}

              {aiTestResult && (
                <Alert
                  severity={aiTestResult.success ? "success" : "error"}
                  icon={aiTestResult.success ? <CheckCircle /> : <Error />}
                >
                  {aiTestResult.success
                    ? `AI configured successfully! Provider: ${aiTestResult.provider}, Model: ${aiTestResult.model}`
                    : `Configuration failed: ${aiTestResult.error}`}
                </Alert>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Settings;
