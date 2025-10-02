import React from "react";
import {
  AppBar,
  Toolbar,
  Typography,
  IconButton,
  Box,
  Chip,
} from "@mui/material";
import { Menu, Storage, SmartToy } from "@mui/icons-material";

const Navbar = ({ onMenuClick, databricksConnected, aiConfigured }) => {
  return (
    <AppBar position="static" elevation={1}>
      <Toolbar>
        <IconButton
          edge="start"
          color="inherit"
          aria-label="menu"
          onClick={onMenuClick}
          sx={{ mr: 2 }}
        >
          <Menu />
        </IconButton>

        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          Databricks PDF Processor
        </Typography>

        <Box display="flex" alignItems="center" gap={2}>
          {/* Connection Status */}
          <Box display="flex" alignItems="center" gap={1}>
            <Storage fontSize="small" />
            <Chip
              label={databricksConnected ? "Connected" : "Disconnected"}
              color={databricksConnected ? "success" : "error"}
              size="small"
              variant="outlined"
              sx={{ color: "white", borderColor: "rgba(255,255,255,0.5)" }}
            />
          </Box>

          {/* AI Status */}
          <Box display="flex" alignItems="center" gap={1}>
            <SmartToy fontSize="small" />
            <Chip
              label={aiConfigured ? "AI Ready" : "AI Not Ready"}
              color={aiConfigured ? "success" : "warning"}
              size="small"
              variant="outlined"
              sx={{ color: "white", borderColor: "rgba(255,255,255,0.5)" }}
            />
          </Box>
        </Box>
      </Toolbar>
    </AppBar>
  );
};

export default Navbar;
