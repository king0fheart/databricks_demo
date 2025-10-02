import React, { useState, useEffect } from "react";
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  Button,
  Chip,
  LinearProgress,
  Alert,
} from "@mui/material";
import { Description, SmartToy, Storage } from "@mui/icons-material";
import { useNavigate } from "react-router-dom";
import { databricksService } from "../services/databricksService";
import toast from "react-hot-toast";

const Dashboard = ({ databricksConnected, aiConfigured }) => {
  const navigate = useNavigate();
  const [stats, setStats] = useState({
    totalPDFs: 0,
    loading: true,
  });
  const [, setRecentPDFs] = useState([]);

  useEffect(() => {
    if (databricksConnected) {
      loadDashboardData();
    }
  }, [databricksConnected]);

  const loadDashboardData = async () => {
    try {
      const pdfList = await databricksService.listPDFs();
      if (pdfList.success) {
        setStats({
          totalPDFs: pdfList.count,
          loading: false,
        });
        setRecentPDFs(pdfList.pdfs.slice(0, 5)); // Show last 5 PDFs
      }
    } catch (error) {
      console.error("Failed to load dashboard data:", error);
      toast.error("Failed to load dashboard data");
      setStats({ totalPDFs: 0, loading: false });
    }
  };

  const getStatusColor = (connected) => {
    return connected ? "success" : "error";
  };

  const getStatusText = (connected) => {
    return connected ? "Connected" : "Not Connected";
  };

  return (
    <Box>
      {/* Header */}
      <Box mb={4}>
        <Typography variant="h4" gutterBottom>
          Dashboard
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Welcome to your Databricks PDF Processing workspace
        </Typography>
      </Box>

      {/* Status Cards */}
      <Grid container spacing={3} mb={4}>
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" mb={2}>
                <Storage color="primary" sx={{ mr: 1 }} />
                <Typography variant="h6">Databricks</Typography>
              </Box>
              <Chip
                label={getStatusText(databricksConnected)}
                color={getStatusColor(databricksConnected)}
                size="small"
              />
              <Typography variant="body2" color="text.secondary" mt={1}>
                Workspace connection status
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" mb={2}>
                <SmartToy color="primary" sx={{ mr: 1 }} />
                <Typography variant="h6">AI Engine</Typography>
              </Box>
              <Chip
                label={getStatusText(aiConfigured)}
                color={getStatusColor(aiConfigured)}
                size="small"
              />
              <Typography variant="body2" color="text.secondary" mt={1}>
                AI query engine status
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" mb={2}>
                <Description color="primary" sx={{ mr: 1 }} />
                <Typography variant="h6">PDFs</Typography>
              </Box>
              {stats.loading ? (
                <LinearProgress />
              ) : (
                <Typography variant="h4" color="primary">
                  {stats.totalPDFs}
                </Typography>
              )}
              <Typography variant="body2" color="text.secondary" mt={1}>
                Total uploaded documents
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Connection Status Alert */}
      {!databricksConnected && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          <Typography variant="body2">
            Databricks is not connected. Please configure your connection in{" "}
            <Button
              size="small"
              onClick={() => navigate("/settings")}
              sx={{ textTransform: "none", p: 0, minWidth: "auto" }}
            >
              Settings
            </Button>{" "}
            to start using the application.
          </Typography>
        </Alert>
      )}

      {databricksConnected && !aiConfigured && (
        <Alert severity="info" sx={{ mb: 3 }}>
          <Typography variant="body2">
            AI engine is not configured. Configure it in{" "}
            <Button
              size="small"
              onClick={() => navigate("/settings")}
              sx={{ textTransform: "none", p: 0, minWidth: "auto" }}
            >
              Settings
            </Button>{" "}
            to enable PDF querying.
          </Typography>
        </Alert>
      )}
    </Box>
  );
};

export default Dashboard;
