import React, { useState, useEffect, useCallback } from "react";
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  LinearProgress,
  Alert,
  Grid,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  ListItemSecondary,
  Chip,
  Paper,
  Switch,
  FormControlLabel,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from "@mui/material";
import {
  CloudUpload,
  Description,
  CheckCircle,
  Error,
  Refresh,
  Delete,
  Visibility,
} from "@mui/icons-material";
import { useDropzone } from "react-dropzone";
import {
  databricksService,
  formatFileSize,
  formatDate,
} from "../services/databricksService";
import toast from "react-hot-toast";

const PDFUpload = () => {
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState(null);
  const [createNotebook, setCreateNotebook] = useState(true);
  const [uploadedPDFs, setUploadedPDFs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedPDF, setSelectedPDF] = useState(null);
  const [previewOpen, setPreviewOpen] = useState(false);

  useEffect(() => {
    loadUploadedPDFs();
  }, []);

  const loadUploadedPDFs = async () => {
    try {
      const response = await databricksService.listPDFs();
      if (response.success) {
        setUploadedPDFs(response.pdfs);
      } else {
        toast.error("Failed to load uploaded PDFs");
      }
    } catch (error) {
      console.error("Failed to load PDFs:", error);
      toast.error("Failed to load PDFs");
    } finally {
      setLoading(false);
    }
  };

  const onDrop = useCallback(
    async (acceptedFiles) => {
      const file = acceptedFiles[0];
      if (!file) return;

      // Validate file type
      if (!file.type.includes("pdf")) {
        toast.error("Please select a PDF file");
        return;
      }

      // Validate file size (max 50MB)
      if (file.size > 50 * 1024 * 1024) {
        toast.error("File size must be less than 50MB");
        return;
      }

      setUploading(true);
      setUploadProgress(0);
      setUploadResult(null);

      try {
        // Simulate progress for better UX
        const progressInterval = setInterval(() => {
          setUploadProgress((prev) => {
            if (prev >= 90) {
              clearInterval(progressInterval);
              return prev;
            }
            return prev + 10;
          });
        }, 200);

        const result = await databricksService.uploadPDF(file, createNotebook);

        clearInterval(progressInterval);
        setUploadProgress(100);
        setUploadResult(result);

        if (result.success) {
          toast.success("PDF uploaded successfully!");
          // Reload the PDF list
          await loadUploadedPDFs();
        } else {
          toast.error(result.error || "Upload failed");
        }
      } catch (error) {
        console.error("Upload failed:", error);
        setUploadResult({
          success: false,
          error: error.message,
        });
        toast.error("Upload failed");
      } finally {
        setUploading(false);
        // Reset progress after a delay
        setTimeout(() => {
          setUploadProgress(0);
        }, 2000);
      }
    },
    [createNotebook]
  );

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      "application/pdf": [".pdf"],
    },
    multiple: false,
    disabled: uploading,
  });

  const handlePreviewPDF = (pdf) => {
    setSelectedPDF(pdf);
    setPreviewOpen(true);
  };

  const handleClosePreview = () => {
    setPreviewOpen(false);
    setSelectedPDF(null);
  };

  const refreshPDFList = () => {
    setLoading(true);
    loadUploadedPDFs();
  };

  return (
    <Box>
      {/* Header */}
      <Box mb={4}>
        <Typography variant="h4" gutterBottom>
          PDF Upload
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Upload PDF documents to your Databricks workspace for AI-powered
          analysis
        </Typography>
      </Box>

      <Grid container spacing={3}>
        {/* Upload Section */}
        <Grid item xs={12} lg={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Upload New PDF
              </Typography>

              {/* Upload Options */}
              {/* <Box mb={3}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={createNotebook}
                      onChange={(e) => setCreateNotebook(e.target.checked)}
                      disabled={uploading}
                    />
                  }
                  label="Create processing notebook"
                />
                <Typography variant="body2" color="text.secondary">
                  Automatically create a Databricks notebook for PDF processing
                </Typography>
              </Box> */}

              {/* Dropzone */}
              <Paper
                {...getRootProps()}
                sx={{
                  p: 4,
                  border: "2px dashed",
                  borderColor: isDragActive ? "primary.main" : "grey.300",
                  backgroundColor: isDragActive
                    ? "action.hover"
                    : "background.paper",
                  cursor: uploading ? "not-allowed" : "pointer",
                  textAlign: "center",
                  transition: "all 0.2s",
                  "&:hover": {
                    borderColor: uploading ? "grey.300" : "primary.main",
                    backgroundColor: uploading
                      ? "background.paper"
                      : "action.hover",
                  },
                }}
              >
                <input {...getInputProps()} />
                <CloudUpload
                  sx={{
                    fontSize: 48,
                    color: isDragActive ? "primary.main" : "text.secondary",
                    mb: 2,
                  }}
                />
                <Typography variant="h6" gutterBottom>
                  {isDragActive
                    ? "Drop the PDF here"
                    : uploading
                    ? "Uploading..."
                    : "Drag & drop a PDF file here"}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  or click to select a file (max 50MB)
                </Typography>
              </Paper>

              {/* Upload Progress */}
              {uploading && (
                <Box mt={3}>
                  <Typography variant="body2" gutterBottom>
                    Uploading... {uploadProgress}%
                  </Typography>
                  <LinearProgress
                    variant="determinate"
                    value={uploadProgress}
                  />
                </Box>
              )}

              {/* Upload Result */}
              {uploadResult && (
                <Alert
                  severity={uploadResult.success ? "success" : "error"}
                  sx={{ mt: 3 }}
                  icon={uploadResult.success ? <CheckCircle /> : <Error />}
                >
                  {uploadResult.success ? (
                    <Box>
                      <Typography variant="body2" gutterBottom>
                        PDF uploaded successfully!
                      </Typography>
                      {uploadResult.workspace_path && (
                        <Typography variant="caption" display="block">
                          Path: {uploadResult.workspace_path}
                        </Typography>
                      )}
                      {uploadResult.notebook_path && (
                        <Typography variant="caption" display="block">
                          Notebook: {uploadResult.notebook_path}
                        </Typography>
                      )}
                    </Box>
                  ) : (
                    <Typography variant="body2">
                      Upload failed: {uploadResult.error}
                    </Typography>
                  )}
                </Alert>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Uploaded PDFs List */}
        <Grid item xs={12} lg={6}>
          <Card>
            <CardContent>
              <Box
                display="flex"
                alignItems="center"
                justifyContent="space-between"
                mb={2}
              >
                <Typography variant="h6">
                  Uploaded PDFs ({uploadedPDFs.length})
                </Typography>
                <IconButton onClick={refreshPDFList} disabled={loading}>
                  <Refresh />
                </IconButton>
              </Box>

              {loading ? (
                <Box display="flex" justifyContent="center" p={3}>
                  <LinearProgress sx={{ width: "100%" }} />
                </Box>
              ) : uploadedPDFs.length === 0 ? (
                <Box textAlign="center" p={3} color="text.secondary">
                  <Description sx={{ fontSize: 48, mb: 2, opacity: 0.5 }} />
                  <Typography variant="body1">No PDFs uploaded yet</Typography>
                  <Typography variant="body2">
                    Upload your first PDF to get started
                  </Typography>
                </Box>
              ) : (
                <List>
                  {uploadedPDFs.map((pdf, index) => (
                    <ListItem
                      key={pdf.workspace_path}
                      divider={index < uploadedPDFs.length - 1}
                      sx={{ px: 0 }}
                    >
                      <ListItemIcon>
                        <Description color="primary" />
                      </ListItemIcon>
                      <ListItemText
                        primary={pdf.name}
                        secondary={
                          <Box>
                            <Typography variant="caption" display="block">
                              Size: {formatFileSize(pdf.size)}
                            </Typography>
                            <Typography variant="caption" display="block">
                              Uploaded: {formatDate(pdf.upload_date)}
                            </Typography>
                            <Typography
                              variant="caption"
                              display="block"
                              sx={{ mt: 0.5 }}
                            >
                              Path: {pdf.workspace_path}
                            </Typography>
                          </Box>
                        }
                      />
                      <Box display="flex" gap={1}>
                        <IconButton
                          size="small"
                          onClick={() => handlePreviewPDF(pdf)}
                          title="View details"
                        >
                          <Visibility />
                        </IconButton>
                      </Box>
                    </ListItem>
                  ))}
                </List>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* PDF Preview Dialog */}
      <Dialog
        open={previewOpen}
        onClose={handleClosePreview}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>PDF Details</DialogTitle>
        <DialogContent>
          {selectedPDF && (
            <Box>
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" gutterBottom>
                    File Name
                  </Typography>
                  <Typography variant="body2" gutterBottom>
                    {selectedPDF.name}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" gutterBottom>
                    File Size
                  </Typography>
                  <Typography variant="body2" gutterBottom>
                    {formatFileSize(selectedPDF.size)}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" gutterBottom>
                    Upload Date
                  </Typography>
                  <Typography variant="body2" gutterBottom>
                    {formatDate(selectedPDF.upload_date)}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" gutterBottom>
                    Status
                  </Typography>
                  <Chip label="Uploaded" color="success" size="small" />
                </Grid>
                <Grid item xs={12}>
                  <Typography variant="subtitle2" gutterBottom>
                    Workspace Path
                  </Typography>
                  <Typography
                    variant="body2"
                    sx={{
                      fontFamily: "monospace",
                      bgcolor: "grey.100",
                      p: 1,
                      borderRadius: 1,
                    }}
                  >
                    {selectedPDF.workspace_path}
                  </Typography>
                </Grid>
              </Grid>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClosePreview}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default PDFUpload;
