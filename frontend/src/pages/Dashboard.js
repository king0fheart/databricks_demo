import React, { useState, useEffect } from 'react';
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
  Paper,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
} from '@mui/material';
import {
  CloudUpload,
  Chat,
  Settings,
  CheckCircle,
  Warning,
  Description,
  SmartToy,
  Storage,
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { databricksService } from '../services/databricksService';
import toast from 'react-hot-toast';

const Dashboard = ({ databricksConnected, aiConfigured }) => {
  const navigate = useNavigate();
  const [stats, setStats] = useState({
    totalPDFs: 0,
    loading: true,
  });
  const [recentPDFs, setRecentPDFs] = useState([]);

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
      console.error('Failed to load dashboard data:', error);
      toast.error('Failed to load dashboard data');
      setStats({ totalPDFs: 0, loading: false });
    }
  };

  const getStatusColor = (connected) => {
    return connected ? 'success' : 'error';
  };

  const getStatusText = (connected) => {
    return connected ? 'Connected' : 'Not Connected';
  };

  const quickActions = [
    {
      title: 'Upload PDF',
      description: 'Upload a new PDF document to Databricks',
      icon: <CloudUpload />,
      action: () => navigate('/upload'),
      enabled: databricksConnected,
      color: 'primary',
    },
    {
      title: 'Chat with PDFs',
      description: 'Ask questions about your uploaded documents',
      icon: <Chat />,
      action: () => navigate('/chat'),
      enabled: databricksConnected && aiConfigured,
      color: 'secondary',
    },
    {
      title: 'Settings',
      description: 'Configure Databricks and AI settings',
      icon: <Settings />,
      action: () => navigate('/settings'),
      enabled: true,
      color: 'default',
    },
  ];

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
            Databricks is not connected. Please configure your connection in{' '}
            <Button
              size="small"
              onClick={() => navigate('/settings')}
              sx={{ textTransform: 'none', p: 0, minWidth: 'auto' }}
            >
              Settings
            </Button>{' '}
            to start using the application.
          </Typography>
        </Alert>
      )}

      {databricksConnected && !aiConfigured && (
        <Alert severity="info" sx={{ mb: 3 }}>
          <Typography variant="body2">
            AI engine is not configured. Configure it in{' '}
            <Button
              size="small"
              onClick={() => navigate('/settings')}
              sx={{ textTransform: 'none', p: 0, minWidth: 'auto' }}
            >
              Settings
            </Button>{' '}
            to enable PDF querying.
          </Typography>
        </Alert>
      )}

      <Grid container spacing={3}>
        {/* Quick Actions */}
        <Grid item xs={12} md={8}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Quick Actions
              </Typography>
              <Grid container spacing={2}>
                {quickActions.map((action, index) => (
                  <Grid item xs={12} sm={6} md={4} key={index}>
                    <Paper
                      elevation={1}
                      sx={{
                        p: 2,
                        cursor: action.enabled ? 'pointer' : 'not-allowed',
                        opacity: action.enabled ? 1 : 0.6,
                        '&:hover': {
                          elevation: action.enabled ? 3 : 1,
                          backgroundColor: action.enabled ? 'action.hover' : 'inherit',
                        },
                        transition: 'all 0.2s',
                      }}
                      onClick={action.enabled ? action.action : undefined}
                    >
                      <Box display="flex" alignItems="center" mb={1}>
                        {action.icon}
                        <Typography variant="subtitle1" sx={{ ml: 1 }}>
                          {action.title}
                        </Typography>
                      </Box>
                      <Typography variant="body2" color="text.secondary">
                        {action.description}
                      </Typography>
                      {!action.enabled && (
                        <Chip
                          label="Setup Required"
                          size="small"
                          color="warning"
                          sx={{ mt: 1 }}
                        />
                      )}
                    </Paper>
                  </Grid>
                ))}
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        {/* Recent PDFs */}
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Recent PDFs
              </Typography>
              {!databricksConnected ? (
                <Typography variant="body2" color="text.secondary">
                  Connect to Databricks to view your PDFs
                </Typography>
              ) : stats.loading ? (
                <LinearProgress />
              ) : recentPDFs.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  No PDFs uploaded yet
                </Typography>
              ) : (
                <List dense>
                  {recentPDFs.map((pdf, index) => (
                    <ListItem key={index} divider={index < recentPDFs.length - 1}>
                      <ListItemIcon>
                        <Description fontSize="small" />
                      </ListItemIcon>
                      <ListItemText
                        primary={pdf.name}
                        secondary={`${pdf.size} • ${pdf.upload_date}`}
                        primaryTypographyProps={{
                          variant: 'body2',
                          noWrap: true,
                        }}
                        secondaryTypographyProps={{
                          variant: 'caption',
                        }}
                      />
                    </ListItem>
                  ))}
                </List>
              )}
              {recentPDFs.length > 0 && (
                <Box mt={2}>
                  <Button
                    size="small"
                    onClick={() => navigate('/upload')}
                    disabled={!databricksConnected}
                  >
                    View All PDFs
                  </Button>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Getting Started */}
      {!databricksConnected && (
        <Card sx={{ mt: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Getting Started
            </Typography>
            <Typography variant="body2" color="text.secondary" mb={2}>
              Follow these steps to set up your Databricks PDF processing workspace:
            </Typography>
            <List>
              <ListItem>
                <ListItemIcon>
                  <CheckCircle color={databricksConnected ? 'success' : 'disabled'} />
                </ListItemIcon>
                <ListItemText
                  primary="Connect to Databricks"
                  secondary="Configure your Databricks workspace connection"
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircle color={aiConfigured ? 'success' : 'disabled'} />
                </ListItemIcon>
                <ListItemText
                  primary="Configure AI Engine"
                  secondary="Set up Databricks AI or OpenAI for PDF querying"
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <Warning color="disabled" />
                </ListItemIcon>
                <ListItemText
                  primary="Upload PDFs"
                  secondary="Upload your first PDF document"
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <Warning color="disabled" />
                </ListItemIcon>
                <ListItemText
                  primary="Start Chatting"
                  secondary="Ask questions about your documents"
                />
              </ListItem>
            </List>
            <Box mt={2}>
              <Button
                variant="contained"
                onClick={() => navigate('/settings')}
                startIcon={<Settings />}
              >
                Go to Settings
              </Button>
            </Box>
          </CardContent>
        </Card>
      )}
    </Box>
  );
};

export default Dashboard;
