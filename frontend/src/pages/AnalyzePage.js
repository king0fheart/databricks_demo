import React, { useState, useEffect } from "react";
import {
  Container,
  Paper,
  Typography,
  Box,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Card,
  CardContent,
  Grid,
  Alert,
  CircularProgress,
  Chip,
  Divider,
} from "@mui/material";
import {
  Description as PdfIcon,
  Psychology as AnalyzeIcon,
  QuestionAnswer as QuestionIcon,
} from "@mui/icons-material";
import { databricksService } from "../services/databricksService";

const AnalyzePage = () => {
  const [availablePDFs, setAvailablePDFs] = useState([]);
  const [selectedPDF, setSelectedPDF] = useState("");
  const [loading, setLoading] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);
  const [results, setResults] = useState({});
  const [error, setError] = useState("");

  // Fixed questions for analysis
  const fixedQuestions = [
    {
      id: "summary",
      question: "What is this document about? Provide a comprehensive summary.",
      icon: <PdfIcon />,
      color: "primary",
    },
    {
      id: "key_points",
      question: "What is the policy number and who is the policyholder?",
      icon: <QuestionIcon />,
      color: "secondary",
    },
    {
      id: "parties",
      question:
        "Who are the main parties involved? (e.g., policyholder, beneficiaries, company)",
      icon: <AnalyzeIcon />,
      color: "success",
    },
    {
      id: "dates_numbers",
      question:
        "What are the important dates, numbers, amounts, or policy details mentioned?",
      icon: <QuestionIcon />,
      color: "warning",
    },
    {
      id: "terms_conditions",
      question:
        "What are the main terms, conditions, or requirements outlined in this document?",
      icon: <PdfIcon />,
      color: "info",
    },
  ];

  useEffect(() => {
    loadPDFs();
  }, []);

  const loadPDFs = async () => {
    try {
      setLoading(true);
      setError("");
      const pdfs = await databricksService.listPDFs();
      setAvailablePDFs(pdfs.pdfs);
    } catch (err) {
      setError("Failed to load PDFs: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleAnalyze = async (questionData) => {
    if (!selectedPDF) {
      setError("Please select a PDF first");
      return;
    }

    try {
      setAnalyzing(true);
      setError("");

      console.log("Analyzing PDF:", {
        pdf_path: selectedPDF,
        question: questionData.question,
        conversation_id: `analyze_${questionData.id}_${Date.now()}`,
      });

      const response = await databricksService.queryPDF({
        pdf_path: selectedPDF,
        question: questionData.question,
        conversation_id: `analyze_${questionData.id}_${Date.now()}`,
      });

      console.log("Analysis response:", response);

      if (response.success) {
        setResults((prev) => ({
          ...prev,
          [questionData.id]: {
            pdf_path: selectedPDF,
            question: questionData.question,
            answer: response.answer,
            timestamp: new Date().toLocaleString(),
          },
        }));
      } else {
        setError(`Analysis failed: ${response.error || "Unknown error"}`);
      }
    } catch (err) {
      console.error("Analysis error:", err);
      setError("Analysis failed: " + err.message);
    } finally {
      setAnalyzing(false);
    }
  };

  const clearResults = () => {
    setResults({});
    setError("");
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      {/* Header */}
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          PDF Document Analysis
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Select a PDF and analyze it with pre-defined questions using
          Databricks AI
        </Typography>
      </Paper>

      {/* PDF Selection */}
      <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Select PDF Document
        </Typography>

        <Box sx={{ display: "flex", gap: 2, alignItems: "center", mb: 2 }}>
          <FormControl fullWidth>
            <InputLabel>Choose PDF to analyze</InputLabel>
            <Select
              value={selectedPDF}
              label="Choose PDF to analyze"
              onChange={(e) => setSelectedPDF(e.target.value)}
              disabled={loading}
            >
              {availablePDFs.map((pdf) => (
                <MenuItem key={pdf.workspace_path} value={pdf.workspace_path}>
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <PdfIcon color="error" />
                    {pdf.display_name || pdf.filename || pdf.name}
                  </Box>
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          <Button
            variant="outlined"
            onClick={loadPDFs}
            disabled={loading}
            sx={{ minWidth: 120 }}
          >
            {loading ? <CircularProgress size={20} /> : "Refresh"}
          </Button>
        </Box>

        {selectedPDF && (
          <Alert severity="info" sx={{ mt: 2 }}>
            <strong>Selected:</strong>{" "}
            {availablePDFs.find((pdf) => pdf.workspace_path === selectedPDF)
              ?.display_name || "PDF Document"}
          </Alert>
        )}

        {availablePDFs.length === 0 && !loading && (
          <Alert severity="warning">
            No PDF files found. Please upload a PDF first using the Upload page.
          </Alert>
        )}
      </Paper>

      {/* Analysis Questions */}
      <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Analysis Questions
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
          Click "Analyze" for each question to get AI-powered insights about
          your document
        </Typography>

        <Grid container spacing={2}>
          {fixedQuestions.map((questionData) => (
            <Grid item xs={12} key={questionData.id}>
              <Card
                variant="outlined"
                sx={{
                  transition: "all 0.2s",
                  "&:hover": {
                    boxShadow: 2,
                    transform: "translateY(-2px)",
                  },
                }}
              >
                <CardContent>
                  <Box
                    sx={{ display: "flex", alignItems: "flex-start", gap: 2 }}
                  >
                    <Box sx={{ color: `${questionData.color}.main`, mt: 0.5 }}>
                      {questionData.icon}
                    </Box>

                    <Box sx={{ flexGrow: 1 }}>
                      <Typography variant="body1" sx={{ mb: 2 }}>
                        {questionData.question}
                      </Typography>

                      <Box
                        sx={{ display: "flex", gap: 1, alignItems: "center" }}
                      >
                        <Button
                          variant="contained"
                          color={questionData.color}
                          onClick={() => handleAnalyze(questionData)}
                          disabled={!selectedPDF || analyzing}
                          startIcon={
                            analyzing ? (
                              <CircularProgress size={16} />
                            ) : (
                              <AnalyzeIcon />
                            )
                          }
                          size="small"
                        >
                          {analyzing ? "Analyzing..." : "Analyze"}
                        </Button>

                        {results[questionData.id] && (
                          <Chip
                            label="✓ Completed"
                            color="success"
                            size="small"
                            variant="outlined"
                          />
                        )}
                      </Box>
                    </Box>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>

        {Object.keys(results).length > 0 && (
          <Box sx={{ mt: 3, display: "flex", justifyContent: "center" }}>
            <Button variant="outlined" color="secondary" onClick={clearResults}>
              Clear All Results
            </Button>
          </Box>
        )}
      </Paper>

      {/* Results */}
      {Object.keys(results).length > 0 && (
        <Paper elevation={2} sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Analysis Results
          </Typography>

          {Object.entries(results).map(([questionId, result]) => {
            const questionData = fixedQuestions.find(
              (q) => q.id === questionId
            );
            return (
              <Box key={questionId} sx={{ mb: 3 }}>
                <Box
                  sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}
                >
                  <Box sx={{ color: `${questionData.color}.main` }}>
                    {questionData.icon}
                  </Box>
                  <Typography variant="subtitle1" fontWeight="bold">
                    {result.question} - ({result.pdf_path})
                  </Typography>
                </Box>

                <Paper
                  variant="outlined"
                  sx={{
                    p: 2,
                    backgroundColor: "grey.50",
                    border: `1px solid`,
                    borderColor: `${questionData.color}.light`,
                  }}
                >
                  <Typography
                    variant="body2"
                    sx={{ whiteSpace: "pre-wrap", lineHeight: 1.6 }}
                  >
                    {result.answer}
                  </Typography>

                  <Divider sx={{ my: 1 }} />

                  <Typography variant="caption" color="text.secondary">
                    Analyzed on: {result.timestamp}
                  </Typography>
                </Paper>
              </Box>
            );
          })}
        </Paper>
      )}

      {/* Error Display */}
      {error && (
        <Alert severity="error" sx={{ mt: 2 }}>
          {error}
        </Alert>
      )}
    </Container>
  );
};

export default AnalyzePage;
