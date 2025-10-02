import React, { useState, useEffect, useRef } from "react";
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Paper,
  Avatar,
  Chip,
  CircularProgress,
  Alert,
  IconButton,
  Divider,
} from "@mui/material";
import {
  Send,
  Person,
  SmartToy,
  Description,
  Clear,
  Refresh,
} from "@mui/icons-material";
import ReactMarkdown from "react-markdown";
import {
  databricksService,
  generateConversationId,
} from "../services/databricksService";
import toast from "react-hot-toast";

const ChatInterface = () => {
  const [messages, setMessages] = useState([]);
  const [currentMessage, setCurrentMessage] = useState("");
  const [selectedPDF, setSelectedPDF] = useState("");
  const [availablePDFs, setAvailablePDFs] = useState([]);
  const [conversationId, setConversationId] = useState(null);
  const [loading, setLoading] = useState(false);
  const [pdfLoading, setPdfLoading] = useState(true);
  const messagesEndRef = useRef(null);

  useEffect(() => {
    loadAvailablePDFs();
    // Generate conversation ID
    setConversationId(generateConversationId());
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  const loadAvailablePDFs = async () => {
    try {
      const response = await databricksService.listPDFs();
      if (response.success) {
        setAvailablePDFs(response.pdfs);
        if (response.pdfs.length > 0) {
          setSelectedPDF(response.pdfs[0].workspace_path);
        }
      } else {
        toast.error("Failed to load PDFs");
      }
    } catch (error) {
      console.error("Failed to load PDFs:", error);
      toast.error("Failed to load PDFs");
    } finally {
      setPdfLoading(false);
    }
  };

  const handleSendMessage = async () => {
    if (!currentMessage.trim() || !selectedPDF) {
      return;
    }

    if (!selectedPDF) {
      toast.error("Please select a PDF first");
      return;
    }

    const userMessage = {
      id: Date.now(),
      type: "user",
      content: currentMessage,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setCurrentMessage("");
    setLoading(true);

    try {
      const response = await databricksService.queryPDF(
        currentMessage,
        selectedPDF,
        conversationId
      );

      const aiMessage = {
        id: Date.now() + 1,
        type: "ai",
        content: response.answer || "No response received",
        timestamp: new Date(),
        metadata: response.metadata,
        success: response.success,
        error: response.error,
      };

      setMessages((prev) => [...prev, aiMessage]);

      if (!response.success) {
        toast.error(response.error || "Failed to get AI response");
      }
    } catch (error) {
      console.error("Failed to send message:", error);
      const errorMessage = {
        id: Date.now() + 1,
        type: "ai",
        content:
          "Sorry, I encountered an error while processing your question.",
        timestamp: new Date(),
        success: false,
        error: error.message,
      };
      setMessages((prev) => [...prev, errorMessage]);
      toast.error("Failed to send message");
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      handleSendMessage();
    }
  };

  const clearConversation = () => {
    setMessages([]);
    setConversationId(generateConversationId());
    toast.success("Conversation cleared");
  };

  const refreshPDFs = () => {
    setPdfLoading(true);
    loadAvailablePDFs();
  };

  const formatTimestamp = (timestamp) => {
    return new Date(timestamp).toLocaleTimeString();
  };

  const MessageBubble = ({ message }) => {
    const isUser = message.type === "user";

    return (
      <Box
        display="flex"
        justifyContent={isUser ? "flex-end" : "flex-start"}
        mb={2}
      >
        <Box
          display="flex"
          alignItems="flex-start"
          maxWidth="70%"
          flexDirection={isUser ? "row-reverse" : "row"}
        >
          <Avatar
            sx={{
              bgcolor: isUser ? "primary.main" : "secondary.main",
              width: 32,
              height: 32,
              mx: 1,
            }}
          >
            {isUser ? (
              <Person fontSize="small" />
            ) : (
              <SmartToy fontSize="small" />
            )}
          </Avatar>

          <Paper
            elevation={1}
            sx={{
              p: 2,
              bgcolor: isUser ? "primary.light" : "background.paper",
              color: isUser ? "primary.contrastText" : "text.primary",
              borderRadius: 2,
            }}
          >
            <Box>
              {isUser ? (
                <Typography variant="body1">{message.content}</Typography>
              ) : (
                <ReactMarkdown>{message.content}</ReactMarkdown>
              )}

              <Box
                display="flex"
                justifyContent="space-between"
                alignItems="center"
                mt={1}
              >
                <Typography variant="caption" sx={{ opacity: 0.7 }}>
                  {formatTimestamp(message.timestamp)}
                </Typography>

                {!isUser && message.metadata && (
                  <Chip
                    label={`${message.metadata.model_used || "AI"}`}
                    size="small"
                    variant="outlined"
                    sx={{ ml: 1 }}
                  />
                )}
              </Box>

              {!isUser && !message.success && message.error && (
                <Alert severity="error" sx={{ mt: 1 }}>
                  <Typography variant="caption">{message.error}</Typography>
                </Alert>
              )}
            </Box>
          </Paper>
        </Box>
      </Box>
    );
  };

  return (
    <Box>
      {/* Header */}
      <Box mb={3}>
        <Typography variant="h4" gutterBottom>
          Chat with PDFs
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Ask questions about your uploaded documents using AI
        </Typography>
      </Box>

      {/* PDF Selection */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box display="flex" alignItems="center" gap={2}>
            <FormControl sx={{ minWidth: 300 }} disabled={pdfLoading}>
              <InputLabel>Select PDF</InputLabel>
              <Select
                value={selectedPDF}
                onChange={(e) => setSelectedPDF(e.target.value)}
                label="Select PDF"
                startAdornment={<Description sx={{ mr: 1 }} />}
              >
                {availablePDFs.map((pdf) => (
                  <MenuItem key={pdf.workspace_path} value={pdf.workspace_path}>
                    {pdf.display_name || pdf.filename || pdf.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <IconButton onClick={refreshPDFs} disabled={pdfLoading}>
              <Refresh />
            </IconButton>

            <Box flexGrow={1} />

            <Button
              startIcon={<Clear />}
              onClick={clearConversation}
              disabled={messages.length === 0}
            >
              Clear Chat
            </Button>
          </Box>

          {pdfLoading && (
            <Box display="flex" alignItems="center" mt={2}>
              <CircularProgress size={16} sx={{ mr: 1 }} />
              <Typography variant="body2" color="text.secondary">
                Loading PDFs...
              </Typography>
            </Box>
          )}

          {!pdfLoading && availablePDFs.length === 0 && (
            <Alert severity="info" sx={{ mt: 2 }}>
              No PDFs found. Please upload a PDF first.
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Chat Messages */}
      <Card
        sx={{
          mb: 3,
          height: "500px",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <CardContent sx={{ flexGrow: 1, overflow: "auto", p: 2 }}>
          {messages.length === 0 ? (
            <Box
              display="flex"
              flexDirection="column"
              alignItems="center"
              justifyContent="center"
              height="100%"
              color="text.secondary"
            >
              <SmartToy sx={{ fontSize: 64, mb: 2, opacity: 0.5 }} />
              <Typography variant="h6" gutterBottom>
                Start a conversation
              </Typography>
              <Typography variant="body2" textAlign="center">
                Select a PDF and ask questions about its content.
                <br />I can help you summarize, extract information, or answer
                specific questions.
              </Typography>
            </Box>
          ) : (
            <Box>
              {messages.map((message) => (
                <MessageBubble key={message.id} message={message} />
              ))}
              {loading && (
                <Box display="flex" justifyContent="flex-start" mb={2}>
                  <Box display="flex" alignItems="center">
                    <Avatar
                      sx={{
                        bgcolor: "secondary.main",
                        width: 32,
                        height: 32,
                        mr: 1,
                      }}
                    >
                      <SmartToy fontSize="small" />
                    </Avatar>
                    <Paper elevation={1} sx={{ p: 2, borderRadius: 2 }}>
                      <Box display="flex" alignItems="center">
                        <CircularProgress size={16} sx={{ mr: 1 }} />
                        <Typography variant="body2">Thinking...</Typography>
                      </Box>
                    </Paper>
                  </Box>
                </Box>
              )}
              <div ref={messagesEndRef} />
            </Box>
          )}
        </CardContent>

        <Divider />

        {/* Message Input */}
        <Box p={2}>
          <Box display="flex" gap={1}>
            <TextField
              fullWidth
              multiline
              maxRows={3}
              placeholder="Ask a question about the selected PDF..."
              value={currentMessage}
              onChange={(e) => setCurrentMessage(e.target.value)}
              onKeyPress={handleKeyPress}
              disabled={loading || !selectedPDF}
              variant="outlined"
              size="small"
            />
            <Button
              variant="contained"
              onClick={handleSendMessage}
              disabled={loading || !currentMessage.trim() || !selectedPDF}
              sx={{ minWidth: "auto", px: 2 }}
            >
              {loading ? <CircularProgress size={20} /> : <Send />}
            </Button>
          </Box>

          {selectedPDF && (
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ mt: 1, display: "block" }}
            >
              Chatting with:{" "}
              {availablePDFs.find((pdf) => pdf.workspace_path === selectedPDF)
                ?.display_name ||
                availablePDFs.find((pdf) => pdf.workspace_path === selectedPDF)
                  ?.filename ||
                availablePDFs.find((pdf) => pdf.workspace_path === selectedPDF)
                  ?.name}
            </Typography>
          )}
        </Box>
      </Card>
    </Box>
  );
};

export default ChatInterface;
