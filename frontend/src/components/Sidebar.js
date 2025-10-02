import React from "react";
import {
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Box,
  Typography,
  Chip,
} from "@mui/material";
import {
  Dashboard,
  CloudUpload,
  Chat,
  Settings,
  Description,
} from "@mui/icons-material";
import { useNavigate, useLocation } from "react-router-dom";

const DRAWER_WIDTH = 280;

const Sidebar = ({ open, onClose, databricksConnected, aiConfigured }) => {
  const navigate = useNavigate();
  const location = useLocation();

  const menuItems = [
    {
      text: "Dashboard",
      icon: <Dashboard />,
      path: "/",
      enabled: true,
    },
    {
      text: "Upload PDFs",
      icon: <CloudUpload />,
      path: "/upload",
      enabled: databricksConnected,
      requiresConnection: true,
    },
    {
      text: "Chat with PDFs",
      icon: <Chat />,
      path: "/chat",
      enabled: databricksConnected && aiConfigured,
      requiresConnection: true,
      requiresAI: true,
    },
    {
      text: "Settings",
      icon: <Settings />,
      path: "/settings",
      enabled: true,
    },
  ];

  const handleNavigation = (path) => {
    navigate(path);
    onClose();
  };

  const isActive = (path) => {
    return location.pathname === path;
  };

  const getItemColor = (item) => {
    if (isActive(item.path)) return "primary";
    if (!item.enabled) return "disabled";
    return "inherit";
  };

  const drawer = (
    <Box sx={{ height: "100%", display: "flex", flexDirection: "column" }}>
      {/* Header */}
      <Box sx={{ p: 3, borderBottom: 1, borderColor: "divider" }}>
        <Box display="flex" alignItems="center" mb={2}>
          <Description color="primary" sx={{ mr: 1 }} />
          <Typography variant="h6" color="primary">
            PDF Processor
          </Typography>
        </Box>
      </Box>

      {/* Navigation Menu */}
      <Box sx={{ flexGrow: 1 }}>
        <List sx={{ pt: 2 }}>
          {menuItems.map((item) => (
            <ListItem key={item.text} disablePadding>
              <ListItemButton
                onClick={() => handleNavigation(item.path)}
                disabled={!item.enabled}
                selected={isActive(item.path)}
                sx={{
                  mx: 1,
                  borderRadius: 1,
                  "&.Mui-selected": {
                    backgroundColor: "primary.light",
                    color: "primary.contrastText",
                    "&:hover": {
                      backgroundColor: "primary.main",
                    },
                  },
                }}
              >
                <ListItemIcon sx={{ color: getItemColor(item) }}>
                  {item.icon}
                </ListItemIcon>
                <ListItemText
                  primary={item.text}
                  sx={{ color: getItemColor(item) }}
                />
                {!item.enabled && (
                  <Chip
                    label={
                      item.requiresAI
                        ? "AI Required"
                        : item.requiresConnection
                        ? "Setup Required"
                        : ""
                    }
                    size="small"
                    color="warning"
                    sx={{ ml: 1 }}
                  />
                )}
              </ListItemButton>
            </ListItem>
          ))}
        </List>
      </Box>
    </Box>
  );

  return (
    <Drawer
      variant="temporary"
      open={open}
      onClose={onClose}
      ModalProps={{
        keepMounted: true, // Better open performance on mobile
      }}
      sx={{
        "& .MuiDrawer-paper": {
          width: DRAWER_WIDTH,
          boxSizing: "border-box",
        },
      }}
    >
      {drawer}
    </Drawer>
  );
};

export default Sidebar;
